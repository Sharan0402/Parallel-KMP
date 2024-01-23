import mpi.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class KmpMpi {
    public static void main(String[] args) throws MPIException, Exception {

        try {
            if (args.length != 2) {
                System.out.println("Please run program with appropriate arguements [pattern] [inputFolder]");
                throw new Exception("No arguments found!");
            }

            MPI.Init(args);

            // initializing mpiSize and myRank, replaced with args
            int mpiSize = MPI.COMM_WORLD.Size();
            int myRank = MPI.COMM_WORLD.Rank();

            long startTime = System.currentTimeMillis();

            // get char array from args
            char[] pattern = args[0].toCharArray();
            List<Integer> foundIndexes = new ArrayList<Integer>();

            // compute LPS Array for pattern
            int[] patternLpsArray = computeLpsArray(pattern);
            if (myRank == 0) {
                System.out.println(pattern);
                for (int k = 0; k < patternLpsArray.length; k++) {
                    System.out.print(patternLpsArray[k]);
                }
                System.out.println("");
            }

            Path currentDirectory = Paths.get(System.getProperty("user.dir"));

            Path inputFolderPath = currentDirectory.resolve(args[1]);
            String outputFileName = "output/output-rank" + myRank + ".txt";
            Path outputFile = currentDirectory.resolve(outputFileName);
            boolean outputFileCreated = false;

            // bulid array with all files in the current directory
            Path[] allFiles = Files.list(inputFolderPath).sorted(Comparator.comparing(Path::getFileName))
                    .toArray(Path[]::new);

            if (allFiles.length == 0) {
                throw new Exception("No files to process in input folder.");
            }
            // calculating number of files to be process by each process
            int remainder = allFiles.length % mpiSize;
            int fileToProcess = allFiles.length / mpiSize;
            int fileIndexBegin = (myRank < remainder) ? fileToProcess * myRank + myRank
                    : fileToProcess * myRank + remainder;
            int fileIndexEnd = (myRank < remainder) ? fileIndexBegin + fileToProcess
                    : fileIndexBegin + fileToProcess - 1;

            // variable indicates processing status for each rank, -1 means no processing
            // occured, 0 process started, 1 process complete
            int localProcessingStatus = -1;

            // process one file at a time
            for (int k = fileIndexBegin; k <= fileIndexEnd; k++) {
                if (myRank > 0 && fileIndexBegin == 0) {
                    continue;
                }
                localProcessingStatus = 0;

                // initalizing list for each file
                foundIndexes = new ArrayList<Integer>();
                String fileName = allFiles[k].toString();
                RandomAccessFile raf = new RandomAccessFile(fileName, "r");
                // variable to hold value of each line
                String line = "";
                // variable to keep track of offset in bytes
                long offset = 0;

                // Read each line from the file until the end is reached
                while ((line = raf.readLine()) != null) {
                    // length of pattern
                    int patternLength = pattern.length;
                    // get lenth of the line
                    int lineLength = line.length();
                    // index for text in line, this pointer will only move forward
                    int i = 0;
                    // index of pattern, this pointer will move back and forth based on LPS array
                    int j = 0;
                    // compute until remaining text is shorter than remaining pattern to be matched
                    while ((lineLength - i) >= (patternLength - j)) {
                        if (pattern[j] == line.charAt(i)) {
                            j++;
                            i++;
                        }
                        if (j == patternLength) {
                            foundIndexes.add((int) offset + i - j);
                            j = patternLpsArray[j - 1];
                        } else if (i < lineLength && pattern[j] != line.charAt(i)) {
                            if (j != 0)
                                j = patternLpsArray[j - 1];
                            else
                                i = i + 1;
                        }
                    }
                    // compute offset for next line
                    offset = raf.getFilePointer();
                }

                raf.close();

                // if indexes are found in file
                if(foundIndexes.size() != 0){
                    PrintWriter writer;
                    // create a new file if file has not been created
                    if(!outputFileCreated){
                        writer = new PrintWriter(new FileWriter(new File(outputFile.toString())));
                        outputFileCreated = true;
                    }else{
                        writer = new PrintWriter(new FileWriter(outputFile.toString(), true));
                    }
                    writer.println(allFiles[k].getFileName().toString());
                    for (Integer index : foundIndexes) {
                        writer.print(index);
                        writer.print(" ");
                    }
                    writer.println();
                    writer.println();
                    writer.close();
                }
            }
            if (localProcessingStatus == 0)
                localProcessingStatus = 1;

            // syncronizing all processess before gathering data in master
            MPI.COMM_WORLD.Barrier();

            int[] gatheredProcessingData = new int[mpiSize];

            // Allgather local data from all processes
            MPI.COMM_WORLD.Allgather(new int[] { localProcessingStatus }, 0, 1, MPI.INT, gatheredProcessingData, 0, 1, MPI.INT);

            if (myRank == 0) {
                // path to final output file
                String finalOutputFile = "output/output.txt";
                Path finalOutputFilePath = currentDirectory.resolve(finalOutputFile);

                // file writer
                BufferedWriter writer = new BufferedWriter(new FileWriter(finalOutputFilePath.toString()));

                for (int u = 0; u < gatheredProcessingData.length; u++) {
                    if (gatheredProcessingData[u] == -1) {
                        System.out.println(" No Processing at rank " + u + ".");
                    } else if (gatheredProcessingData[u] == 0) {
                        System.out.println("Processing at rank " + u + " was incomplete!");
                    } else {
                        //path to ouput
                        String outputFromRank = "output/output-rank" + u + ".txt";
                        Path file = currentDirectory.resolve(outputFromRank);

                        // file reader
                        BufferedReader reader = new BufferedReader(new FileReader(file.toString()));

                        // Read and write the content of the file
                        String line;
                        while ((line = reader.readLine()) != null) {
                            writer.write(line);
                            writer.newLine();
                        }

                        // Close the reader for the current file
                        reader.close();

                        System.out.println("Processing at rank " + u + " is Complete!");
                    }
                }

                writer.close();

                System.out.println("Output.txt Generation complete.");
            }
            
            if (myRank == 0) {
                // stop timer and print elapsed time
                long stopTime = System.currentTimeMillis();
                System.out.println("Elapsed time: " + (stopTime - startTime));
            }
            MPI.Finalize();

        } catch (IOException e) {
            //print an error message
            e.printStackTrace();
        }
    }

    // method to compute LPS array of pattern, retuns integer array with LPS for
    // patern
    public static int[] computeLpsArray(char[] pattern) {
        int[] lpsArray = new int[pattern.length];
        // variable to track length of longest previous prefix
        int lengthOfPreviousLongestPrefix = 0;
        // counter variable to point to each character in pattern
        int counter = 1;
        // lps of first character is always 0
        lpsArray[0] = 0;

        // compute lps of pattern from 1 to length - 1
        while (counter <= pattern.length - 1) {
            // when current character matches to character from previous longest prefix
            if (pattern[counter] == pattern[lengthOfPreviousLongestPrefix]) {
                // increase length of longest previous prefix
                lengthOfPreviousLongestPrefix++;
                // update lps array for current character
                lpsArray[counter] = lengthOfPreviousLongestPrefix;
                // move to next character in pattern
                counter++;
            } else { // if previous longest prefix is a missmatch
                // if previous longest prefix is not start, check if previous prefexes are match
                if (lengthOfPreviousLongestPrefix != 0) {
                    lengthOfPreviousLongestPrefix = lpsArray[lengthOfPreviousLongestPrefix - 1];
                } else {// if previous start prefix is 0, set value for current character to 0 and
                        // increment counter
                    lpsArray[counter] = lengthOfPreviousLongestPrefix;
                    counter++;
                }
            }
        }
        return lpsArray;
    }
}