package com.scaleunlimited.snippets;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;

import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.NullSinkTap;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class DistributedCopy {

    private static class CopyFile extends BaseOperation<NullContext> implements Buffer<NullContext> {

        @Override
        public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
            // TODO get next filename, extract metadata, construct destination name,
            // save metadata, etc.
        }
        
        
    }
    
    
    private static Flow<?> createFlow(String sourceFile, String destDir) {
        
        // We've 
        
        Tap<?, ?, ?> sourceTap = new Hfs(new TextLine(), sourceFile);
        
        Pipe p = new Pipe("filenames");
        p = new GroupBy(p);
        p = new Every(p, new CopyFile());
        
        Tap<?, ?, ?> sinkTap = new NullSinkTap();
        
        FlowDef fd = new FlowDef();
        fd.setName("Copy to SOR");
        fd.addSource(p, sourceTap);
        fd.addTailSink(p, sinkTap);
        
        return new HadoopFlowConnector().connect(fd);
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        String sourceDirname = args[0];
        String destDir = args[1];
        
        try {
            Configuration conf = new Configuration();
            Path sourceDirPath = new Path(sourceDirname);
            FileSystem sourceFS = sourceDirPath.getFileSystem(conf);
            
            Path sourceFilesPath = null;
            if (sourceFS.isFile(sourceDirPath)) {
                // We assume this is a file containing the list of paths
                sourceFilesPath = sourceDirPath;
            } else {
                List<String> sourceFiles = new ArrayList<String>();
                makeSourceFiles(sourceFS, sourceDirPath, sourceFiles);
                // Write sourceFiles out to a temp file in HDFS, and set
                // sourceFilesPath to that file.
                DFSClient sourceClient = new DFSClient(conf);
                Path workingDirPath = sourceFS.getWorkingDirectory();
                sourceFilesPath = new Path(workingDirPath, UUID.randomUUID().toString());
                OutputStream os = sourceClient.create(sourceFilesPath.toString(), true);
                IOUtils.writeLines(sourceFiles, "UTF-8", os);
                os.close();
            }
            
            Flow<?> f = createFlow(sourceFilesPath.toString(), destDir);
            f.complete();
        } catch (Exception e) {
            System.err.println("Exception running tool: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }

    }

    private static void makeSourceFiles(FileSystem sourceFS, Path sourceDirPath, List<String> sourceFiles) throws IOException {
        FileStatus[] filesStatus = sourceFS.listStatus(sourceDirPath);
        for (FileStatus fileStatus : filesStatus) {
            if (fileStatus.isDir()) {
                makeSourceFiles(sourceFS, fileStatus.getPath(), sourceFiles);
            } else {
                sourceFiles.add(fileStatus.getPath().toString());
            }
        }
    }

}
