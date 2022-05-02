package com.custom.postprocessing.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PostProcessUtil {
	
	Logger logger = LoggerFactory.getLogger(PostProcessUtil.class);

	public String getFileType(String fileType) {
		if (fileType.matches(".*[0-9].*")) {
			fileType = "page" + fileType;
		}
		return fileType;
	}
	
	public File completeTxtFile(String currentDate, Set<String> fileNames) {
		File file = null;
		try {
			String documentFileName = "completed-" + currentDate + ".txt";
			file = new File(documentFileName);
			FileOutputStream outputStream = new FileOutputStream(file);
			PrintWriter writer = new PrintWriter(outputStream);
			writer.println("process file type summary" + '\n');
			for (String fileName : fileNames) {
				writer.println(fileName);
			}
			writer.close();
			return file;
		} catch (Exception exception) {
			logger.info("Exception:" + exception.getMessage());
		}
		return file;
	}
}
