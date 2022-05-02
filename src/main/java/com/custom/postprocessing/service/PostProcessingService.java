package com.custom.postprocessing.service;

import static com.custom.postprocessing.constant.PostProcessingConstant.ARCHIVE_DIRECTORY;
import static com.custom.postprocessing.constant.PostProcessingConstant.ARCHIVE_VALUE;
import static com.custom.postprocessing.constant.PostProcessingConstant.BANNER_DIRECTORY;
import static com.custom.postprocessing.constant.PostProcessingConstant.BANNER_PAGE;
import static com.custom.postprocessing.constant.PostProcessingConstant.EMPTY_SPACE;
import static com.custom.postprocessing.constant.PostProcessingConstant.PCL_EXTENSION;
import static com.custom.postprocessing.constant.PostProcessingConstant.PDF_EXTENSION;
import static com.custom.postprocessing.constant.PostProcessingConstant.PRINT_DIRECTORY;
import static com.custom.postprocessing.constant.PostProcessingConstant.PROCESSED_DIRECTORY;
import static com.custom.postprocessing.constant.PostProcessingConstant.SPACE_VALUE;
import static com.custom.postprocessing.constant.PostProcessingConstant.TRANSIT_DIRECTORY;
import static com.custom.postprocessing.constant.PostProcessingConstant.XML_EXTENSION;
import static com.custom.postprocessing.constant.PostProcessingConstant.XML_TYPE;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.custom.postprocessing.constant.PostProcessingConstant;
import com.custom.postprocessing.scheduler.PostProcessingScheduler;
import com.custom.postprocessing.util.EmailUtil;
import com.custom.postprocessing.util.FTPServerUtility;
import com.custom.postprocessing.util.PostProcessUtil;
import com.custom.postprocessing.util.ZipUtility;
import com.groupdocs.conversion.Converter;
import com.groupdocs.conversion.filetypes.FileType;
import com.groupdocs.conversion.options.convert.ConvertOptions;
/*import com.groupdocs.conversion.Converter;
import com.groupdocs.conversion.filetypes.FileType;
import com.groupdocs.conversion.options.convert.ConvertOptions;*/
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

/**
 * @author kumar.charanswain
 *
 */

@Service
public class PostProcessingService {

	Logger logger = LoggerFactory.getLogger(PostProcessingScheduler.class);

	@Value("${blob.account.name.key}")
	private String connectionNameKey;

	@Value("${blob.container.name}")
	private String containerName;

	@Value("#{'${state.allow.type}'.split(',')}")
	private List<String> stateAllowType;

	@Value("#{'${page.type}'.split(',')}")
	private List<String> pageTypeList;

	@Value("${sheet.number.type}")
	private String sheetNbrType;

	@Value("${ftp.server.name}")
	private String ftpHostName;

	@Value("${ftp.server.port}")
	private int ftpPort;

	@Value("${ftp.server.username}")
	private String ftpUserName;

	@Value("${ftp.server.password}")
	private String ftpPassword;

	@Autowired
	FTPServerUtility ftpServerUtility;

	@Autowired
	EmailUtil emailUtil;

	@Autowired
	private PostProcessUtil postProcessUtil;

	@Autowired
	private Environment environment;

	List<String> pclFileList = new LinkedList<String>();

	public String smartComPostProcessing() {
		String messageInfo = "smart comm post processing successfully";
		try {
			CloudBlobContainer container = containerInfo();
			String currentDate = currentDateTime();
			CloudBlobDirectory transitDirectory = getDirectoryName(container, TRANSIT_DIRECTORY,
					currentDate + "-" + PRINT_DIRECTORY);
			String transitTargetDirectory = TRANSIT_DIRECTORY + "/" + currentDate + "-";
			if (moveFileToTargetDirectory(currentDate, PRINT_DIRECTORY, transitTargetDirectory)) {
				messageInfo = processMetaDataInputFile(container, transitDirectory, currentDate);
			} else {
				messageInfo = "no file for post processing";
			}
		} catch (Exception exception) {
			logger.info("Exception:" + exception.getMessage());
			messageInfo = "error in copy file to blob directory";
		}
		return messageInfo;
	}

	public void archivePostProcessing() {
		try {
			String currentDate = currentDateTime();
			String targetDirectory = TRANSIT_DIRECTORY + "/" + currentDate + "-";
			moveFileToTargetDirectory(currentDate, ARCHIVE_DIRECTORY, targetDirectory);
			zipFileTransferToFTPArchive(ARCHIVE_DIRECTORY, targetDirectory + ARCHIVE_DIRECTORY, currentDate);
		} catch (Exception exception) {
			logger.info("error in archive file:" + exception.getMessage());
		}
	}

	private boolean moveFileToTargetDirectory(String currentDate, String sourceDirectory, String targetDirectory) {
		boolean moveSuccess = false;
		BlobContainerClient blobContainerClient = getBlobContainerClient(connectionNameKey, containerName);
		Iterable<BlobItem> listBlobs = blobContainerClient.listBlobsByHierarchy(sourceDirectory);
		for (BlobItem blobItem : listBlobs) {
			BlobClient dstBlobClient = blobContainerClient.getBlobClient(targetDirectory + blobItem.getName());
			BlobClient srcBlobClient = blobContainerClient.getBlobClient(blobItem.getName());
			dstBlobClient.copyFromUrl(srcBlobClient.getBlobUrl());
			// srcBlobClient.delete();
			moveSuccess = true;
		}
		return moveSuccess;
	}

	public void zipFileTransferToFTPArchive(String directoryName, String targetDirectory, String currentDate)
			throws IOException {
		FTPClient ftpClient = new FTPClient();
		try {
			CloudBlobContainer container = containerInfo();
			BlobContainerClient blobContainerClient = getBlobContainerClient(connectionNameKey, containerName);
			CloudBlobDirectory transitDirectory = getDirectoryName(container, TRANSIT_DIRECTORY + "/",
					currentDate + "-" + ARCHIVE_VALUE);
			Iterable<BlobItem> listBlobs = blobContainerClient.listBlobsByHierarchy(directoryName);
			List<String> files = new LinkedList<String>();
			String archiveZipFileName = currentDate + "-" + ARCHIVE_VALUE;
			List<String> archiveZipFileNames = new LinkedList<String>();
			for (BlobItem blobItem : listBlobs) {
				String fileNames[] = StringUtils.split(blobItem.getName(), "/");
				String fileName = fileNames[fileNames.length - 1];
				File file = new File(fileName);
				CloudBlockBlob blob = transitDirectory.getBlockBlobReference(fileName);
				blob.downloadToFile(file.getPath());
				files.add(fileName);
			}
			ZipUtility zipUtility = new ZipUtility();
			zipUtility.zipProcessing(files, archiveZipFileName);
			archiveZipFileNames.add(archiveZipFileName);
			ftpClient = ftpServerUtility.getFtpClient(ftpHostName, ftpPort, ftpUserName, ftpPassword);
			Set<String> txtFileNames = new LinkedHashSet<String>();
			ftpServerUtility.fileTranserToFTPServer(ftpClient, archiveZipFileNames, txtFileNames, currentDate);
			deleteFiles(files);
		} catch (Exception exception) {
			logger.info("exception:" + exception.getMessage());
		} finally {
			ftpClient.disconnect();
		}
	}

	public String processMetaDataInputFile(CloudBlobContainer container, CloudBlobDirectory transitDirectory,
			String currentDate) throws Exception {
		ConcurrentHashMap<String, List<String>> postProcessMap = new ConcurrentHashMap<String, List<String>>();
		String message = "smart comm post processing successfully";
		try {
			Iterable<ListBlobItem> blobList = transitDirectory.listBlobs();

			for (ListBlobItem blobItem : blobList) {
				String fileName = getFileNameFromBlobURI(blobItem.getUri()).replace(SPACE_VALUE, EMPTY_SPACE);
				System.out.println("fileName:" + fileName);
				boolean stateType = checkStateType(fileName);
				if (stateType) {
					if (StringUtils.equalsIgnoreCase(FilenameUtils.getExtension(fileName), XML_TYPE)) {
						continue;
					}
					String fileNameNoExt = FilenameUtils.removeExtension(fileName);
					String[] stateAndSheetNameList = StringUtils.split(fileNameNoExt, "_");
					String stateAndSheetName = stateAndSheetNameList.length > 0
							? stateAndSheetNameList[stateAndSheetNameList.length - 1]
							: "";
					prepareMap(postProcessMap, stateAndSheetName, fileName);
				} else if (checkPageType(fileName)) {
					if (PostProcessingConstant.PDF_TYPE.equals(FilenameUtils.getExtension(fileName))) {
						continue;
					}
					prepareMap(postProcessMap, getSheetNumber(fileName, blobItem),
							StringUtils.replace(fileName, XML_EXTENSION, PDF_EXTENSION));
				} else {
					logger.info("unable to process:invalid document type ");
				}
			}
			if (postProcessMap.size() > 0) {
				message = mergePDF(postProcessMap, currentDate);
			} else {
				message = "unable to process :invalid state/document name";
			}
		} catch (Exception exception) {
			logger.info("Exception found:" + exception.getMessage());
		}
		return message;
	}

	private String getSheetNumber(String fileName, ListBlobItem blobItem) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			File file = new File(fileName);
			CloudBlob cloudBlob = (CloudBlob) blobItem;
			cloudBlob.downloadToFile(file.getPath());
			Document document = builder.parse(file);
			document.getDocumentElement().normalize();
			Element root = document.getDocumentElement();
			int sheetNumber = Integer.parseInt(root.getAttribute(sheetNbrType));
			if (sheetNumber <= 10) {
				file.delete();
				return String.valueOf(sheetNumber);
			}
			file.delete();
		} catch (Exception exception) {
			logger.info("Exception found:" + exception.getMessage());
		}
		return PostProcessingConstant.MULTIPAGE;
	}

	public BlobContainerClient getBlobContainerClient(String connectionNameKey, String containerName) {
		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionNameKey)
				.buildClient();
		BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
		return blobContainerClient;
	}

	// post merge PDF
	public String mergePDF(ConcurrentHashMap<String, List<String>> postProcessMap, String currentDate)
			throws IOException {
		String message = "smart comm post processing successfully";
		List<String> fileNameList = new LinkedList<String>();
		Set<String> txtFileNames = new LinkedHashSet<String>();
		CloudBlobContainer container = containerInfo();
		for (String fileType : postProcessMap.keySet()) {
			try {
				PDFMergerUtility PDFMerger = new PDFMergerUtility();
				fileNameList = postProcessMap.get(fileType);
				String bannerFileName = getBannerPage(fileType);
				File bannerFile = new File(bannerFileName);
				PDFMerger.addSource(bannerFileName);
				Collections.sort(fileNameList);
				CloudBlobDirectory transitDirectory = getDirectoryName(container, TRANSIT_DIRECTORY + "/",
						currentDate + "-" + PRINT_DIRECTORY);
				for (String fileName : fileNameList) {
					File file = new File(fileName);
					CloudBlockBlob blob = transitDirectory.getBlockBlobReference(fileName);
					blob.downloadToFile(file.getAbsolutePath());
					PDFMerger.addSource(file.getPath());
				}
				fileType = postProcessUtil.getFileType(fileType);
				String mergePdfFile = fileType + "-merge" + "-" + currentDate + PDF_EXTENSION;
				PDFMerger.setDestinationFileName(mergePdfFile);
				PDFMerger.mergeDocuments();
				convertPDFToPCL(mergePdfFile, currentDate, fileType);
				bannerFile.delete();
				new File(mergePdfFile).delete();
				deleteFiles(fileNameList);
			} catch (StorageException storageException) {
				logger.info("file not found for processing");
				if (fileNameList.size() > 0) {
					deleteFiles(fileNameList);
				}
				continue;
			} catch (Exception exception) {
				logger.info("Exception:" + exception.getMessage());
			}
		}
		if (postProcessMap.size() > 0) {
			emailUtil.emailProcess(postProcessMap, currentDate);
		}
		FTPClient ftpClient = ftpServerUtility.getFtpClient(ftpHostName, ftpPort, ftpUserName,
				environment.getRequiredProperty("ftp.server.password"));
		if (pclFileList.size() > 0 && txtFileNames.size() > 0) {
			ftpServerUtility.fileTranserToFTPServer(ftpClient, pclFileList, txtFileNames, currentDate);
		}
		return message;
	}

	// post processing PDF to PCL conversion
	public void convertPDFToPCL(String mergePdfFile, String currentDate, String fileType) throws IOException {
		try {

			String outputPclFile = FilenameUtils.removeExtension(mergePdfFile.toString()) + PCL_EXTENSION;
			Converter converter = new Converter(mergePdfFile);
			ConvertOptions<?> convertOptions = FileType.fromExtension("pcl").getConvertOptions();
			converter.convert(outputPclFile, convertOptions);
			copyFileToProcessedDirectory(outputPclFile);
			pclFileList.add(outputPclFile);

		} catch (Exception exception) {
			logger.info("Exception:" + exception.getMessage());
		}
	}

	public void copyFileToProcessedDirectory(String fileName) {
		try {
			CloudBlobContainer container = containerInfo();
			CloudBlobDirectory processDirectory = getDirectoryName(container, TRANSIT_DIRECTORY, PROCESSED_DIRECTORY);
			File outputFileName = new File(fileName);
			CloudBlockBlob processSubDirectoryBlob = processDirectory.getBlockBlobReference(fileName);
			FileInputStream fileInputStream = new FileInputStream(outputFileName);
			processSubDirectoryBlob.upload(fileInputStream, outputFileName.length());
			fileInputStream.close();
		} catch (Exception exception) {
			logger.info("Exception:" + exception.getMessage());
		}
	}

	public boolean checkStateType(String fileName) {
		for (String state : stateAllowType) {
			if (fileName.contains(state)) {
				return true;
			}
		}
		return false;
	}

	public int totalNumberPages(String fileName) throws IOException {
		PDDocument pdfDocument = PDDocument.load(new File(fileName));
		return pdfDocument.getPages().getCount();
	}

	public boolean checkPageType(String fileName) {
		for (String pageType : pageTypeList) {
			if (fileName.contains(pageType)) {
				return true;
			}
		}
		return false;
	}

	public void deleteFiles(List<String> fileNameList) throws IOException {
		for (String fileName : fileNameList) {
			File file = new File(fileName);
			file.delete();
		}
	}

	public void prepareMap(ConcurrentHashMap<String, List<String>> postProcessMap, String key, String fileName)
			throws IOException {
		if (postProcessMap.containsKey(key)) {
			List<String> existingFileNameList = postProcessMap.get(key);
			existingFileNameList.add(fileName);
			postProcessMap.put(key, existingFileNameList);
		} else {
			List<String> existingFileNameList = new ArrayList<String>();
			existingFileNameList.add(fileName);
			postProcessMap.put(key, existingFileNameList);
		}
	}

	public String getBannerPage(String key)
			throws URISyntaxException, StorageException, FileNotFoundException, IOException {
		CloudBlobContainer container = containerInfo();
		CloudBlobDirectory transitDirectory = getDirectoryName(container, BANNER_DIRECTORY, "");
		String bannerFileName = BANNER_PAGE + key + PDF_EXTENSION;
		CloudBlockBlob blob = transitDirectory.getBlockBlobReference(bannerFileName);
		File source = new File(bannerFileName);
		blob.downloadToFile(source.getAbsolutePath());
		return bannerFileName;
	}

	public CloudBlobContainer containerInfo() {
		CloudBlobContainer container = null;
		try {
			CloudStorageAccount account = CloudStorageAccount.parse(connectionNameKey);
			CloudBlobClient serviceClient = account.createCloudBlobClient();
			container = serviceClient.getContainerReference(containerName);
		} catch (Exception exception) {
			logger.info("Exception:" + exception.getMessage());
		}
		return container;
	}

	public String currentDateTime() {
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(date);
	}

	public CloudBlobDirectory getDirectoryName(CloudBlobContainer container, String directoryName,
			String subDirectoryName) throws URISyntaxException {
		CloudBlobDirectory cloudBlobDirectory = container.getDirectoryReference(directoryName);
		if (StringUtils.isBlank(subDirectoryName)) {
			return cloudBlobDirectory;
		}
		return cloudBlobDirectory.getDirectoryReference(subDirectoryName);
	}

	private String getFileNameFromBlobURI(URI uri) {
		String[] fileNameList = uri.toString().split(PostProcessingConstant.FILE_SEPARATION);
		Optional<String> fileName = Optional.empty();
		if (fileNameList.length > 1)
			fileName = Optional.ofNullable(fileNameList[fileNameList.length - 1]);
		return fileName.get();
	}
}