package com.ksc.wordcount.datasourceapi;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class UnsplitFileFormat implements FileFormat {

        @Override
        public boolean isSplitable(String filePath) {
            return true;
        }


        @Override
        public PartionFile[] getSplits(String filePath, long size) {
            List<PartionFile> partiongFileList=new ArrayList<>();
            //todo done 学生实现 driver端切分split的逻辑
            File file=new File(filePath);
            int partionId=0;
            //如果是个文件夹的话，就遍历里面所有的文件
            if(file.isDirectory()){
                File[] files=file.listFiles();
                for(File f:files){
                    FileSplit[] fileSplit = {new FileSplit(f.getAbsolutePath(), 0, f.length())};
                    partiongFileList.add(new PartionFile(partionId,fileSplit));
                    partionId++;
                }
            }
//            FileSplit fileSplit=new FileSplit(filePath,0,file.length());
//            PartionFile partionFile=new PartionFile(0,new FileSplit[]{fileSplit});
//            partiongFileList.add(partionFile);

            return partiongFileList.toArray(new PartionFile[partiongFileList.size()]);
        }

    @Override
    public PartionReader createReader() {
        return new TextPartionReader();
    }

    @Override
    public PartionWriter createWriter(String destPath, int partionId) {
        return new TextPartionWriter(destPath, partionId);
    }


}
