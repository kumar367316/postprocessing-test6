package com.custom.postprocessing.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FTPServerUtility {

	Logger logger = LoggerFactory.getLogger(FTPServerUtility.class);

	@Autowired
	private PostProcessUtil postProcessUtil;
	
	// file transfer to ftp server
	public void fileTranserToFTPServer(FTPClient ftpClient, List<String> pclFileList,
			Set<String> txtDocFileNames,String currentDate) throws IOException {
		FileInputStream fileInputStream = null;
		try {
			for (String fileName : pclFileList) {
				fileInputStream = new FileInputStream(fileName);
				ftpClient.storeFile(fileName, fileInputStream);
				fileInputStream.close();
				File archiveFile = new File(fileName);
				archiveFile.delete();
			}
			if (txtDocFileNames.size() > 0) {
				File completeTxtFile = postProcessUtil.completeTxtFile(currentDate, txtDocFileNames);
				fileInputStream = new FileInputStream(completeTxtFile);
				ftpClient.storeFile(completeTxtFile.toString(), fileInputStream);
				fileInputStream.close();
				completeTxtFile.delete();
			}
			
		} catch (Exception exception) {
			logger.info("Exception:" + exception.getMessage());
		} finally {
			ftpClient.disconnect();
		}
	}
	

	public String currentDateTime() {
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(date);
	}

	public FTPClient getFtpClient(String server, int port, String userName, String password) {
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.connect(server, port);
			ftpClient.login(userName, password);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			ftpClient.makeDirectory(currentDateTime());
			ftpClient.changeWorkingDirectory(currentDateTime());
		} catch (Exception exception) {
			logger.info("exception:" + exception.getMessage());
		}
		return ftpClient;
	}

	public void documentTxtFileProcess(Map<String, String> documentTxtFileMap, String fileName, FTPClient ftpClient) {
		try {
			String fileType = documentTxtFileMap.get(fileName);
			String documentFileName = "completed-"+ currentDateTime()+ ".txt";
			File file = new File(documentFileName);
			FileOutputStream outputStream = new FileOutputStream(file);
			PrintWriter writer = new PrintWriter(outputStream);
			writer.println(fileType + " completed");
			writer.close();
			FileInputStream fileInputStream = new FileInputStream(file.getPath());
			ftpClient.storeFile(file.toString(), fileInputStream);
			fileInputStream.close();
			file.delete();
		} catch (Exception exception) {
			logger.info("exception:" + exception.getMessage());
		}
	}
}
