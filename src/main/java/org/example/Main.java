package org.example;

import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        if (args.length < 1) {
            System.out.println("Не указано имя файла с исходными данными!");
            return;
        }
        // read all data and process it
        List<Long> linesOffsets = new ArrayList<>();
        linesOffsets.add(0L);
        List<List<Long>> lines = new ArrayList<>();
        readAndProcessData(args[0], linesOffsets, lines);
//        logTimeFromStart(startTime, "End of reading and processing data!");

        // get all information about same values in the same columns but from different lines
        List<Map<Long, List<Integer>>> corrTable = new ArrayList<>();
        List<Integer> lineGroups = new ArrayList<>(linesOffsets.size());
        getConnections(lines, lineGroups, corrTable);
//        logTimeFromStart(startTime, "End of parsing data and making table of connections!");

        // find all groups with lines with same values in the same columns
        List<Set<Integer>> groups = new ArrayList<>();
        findAllGroups(lines, lineGroups, groups, corrTable);
        // sort groups by size
        groups.sort(Comparator.comparingInt(Set::size));
        logTimeFromStart(startTime, "End of calculating groups");

        // write down results of program
        writeResultsToFile(args[0], "output.txt", linesOffsets, groups);
        logTimeFromStart(startTime, "End of program");
    }

    public static void logTimeFromStart(long startTime, String event) {
        System.out.println(event);
        long endTime = System.currentTimeMillis();
        System.out.println("That took " + (endTime - startTime) + " milliseconds");
    }

    public static void readAndProcessData(String inputFileName, List<Long> linesOffsets, List<List<Long>> lines) {
        // create reader
        try (RandomAccessFile reader = new RandomAccessFile(inputFileName, "r")) {
            String line;
            Set<Integer> setOfLinesHashCodes = new HashSet<>();
            // read line
            while ((line = reader.readLine()) != null) {
                // parse and write data in lines list
                linesOffsets.add(reader.getFilePointer());
                processLine(line, lines, setOfLinesHashCodes);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void processLine(String line, List<List<Long>> lines, Set<Integer> setOfLines) {
        // get hashcode of line and check if the same line was before
        // this check could be more complex in case of different space symbols and any other problems
        //  line.matches(".*\\d.*") ->
        //      this for the way to not skip lines with empty values - as each line of this type should be in its own group
        Integer lineHashCode = line.hashCode();
        if (setOfLines.contains(lineHashCode) && line.matches(".*\\d.*") || line.isBlank()) {
            lines.add(new ArrayList<>());
            return;
        }

        // split elements by ";"
        List<String> elements = new ArrayList<>(Arrays.stream(line.split(";")).toList());
        if (elements.isEmpty()) {
            lines.add(new ArrayList<>());
            return;
        }
        List<Long> parsedElements = new ArrayList<>(elements.size());
        // for every element:
        for (int i = 0; i < elements.size(); i++) {
            String element = elements.get(i);
            // check if element is in right format
            //  (regular expression was made on the basis of lng.txt)
            if (element.isBlank()) {
                parsedElements.add(null);
                continue;
            }
            if (!element.matches("\"[0-9]{0,13}(\\.[0-9]*)?\"")) {
                lines.add(new ArrayList<>());
                return;
            } else {
                // parse element and add to list
                String elWithoutQuotes = element.subSequence(1, element.length() - 1).toString();
                if (elWithoutQuotes.isBlank()) {
                    parsedElements.add(null);
                } else {
                    parsedElements.add(i, Long.parseLong(elWithoutQuotes));
                }
            }
        }
        // add line hash code to set
        setOfLines.add(lineHashCode);
        // add parsed line to list
        lines.add(parsedElements);
    }

    public static void getConnections(List<List<Long>> lines, List<Integer> lineGroups,
                                      List<Map<Long, List<Integer>>> corrTable) {
        for (int i = 0; i < lines.size(); i++) {
            List<Long> line = lines.get(i);
            // initialize group for every line with -1 if group has elements and -2 if it has no
            lineGroups.add(line.isEmpty() ? -2 : -1);

            // for every column collect unique values and mark in which lines they are
            for (int j = 0; j < line.size(); j++) {
                Long element = line.get(j);
                // initialize map for column
                if (corrTable.size() < j + 1) {
                    corrTable.add(new HashMap<>());
                }
                // skip empty values
                if (element == null) {
                    continue;
                }
                // put element and number of line to corresponding map element
                Map<Long, List<Integer>> column = corrTable.get(j);
                if (!column.containsKey(element)) {
                    column.put(element, new ArrayList<>(List.of(i)));
                } else {
                    List<Integer> columnGroup = column.get(element);
                    columnGroup.add(i);
                }
            }
        }
    }

    public static void findAllGroups(List<List<Long>> lines, List<Integer> lineGroups,
                                     List<Set<Integer>> groups, List<Map<Long, List<Integer>>> corrTable) {
        // for every line:
        for (int i = 0; i < lines.size(); i++) {
            // skip if line is already in any group
            if (lineGroups.get(i) != -1) {
                continue;
            }
            // create queue with lines that are in the current group but hasn't been processed yet
            Queue<Integer> linesToCheck = new LinkedList<>();
            // init group with current line
            Set<Integer> currentGroup = new HashSet<>();
            currentGroup.add(i);
            // init queue with lines connected directly to current line
            addLinesToGroup(i, groups.size(), linesToCheck, currentGroup, lines, lineGroups, corrTable);
            // process all lines connected to current line:
            //  find and add to queue all their connections (indirect connections to current line)
            Integer curLine;
            while ((curLine = linesToCheck.poll()) != null) {
                addLinesToGroup(curLine, groups.size(), linesToCheck, currentGroup, lines, lineGroups, corrTable);
            }
            // add found group
            groups.add(currentGroup);
        }
    }

    public static void addLinesToGroup(Integer curLine, Integer groupNumber,
                                       Queue<Integer> linesToCheck, Set<Integer> currentGroup,
                                       List<List<Long>> lines, List<Integer> lineGroups,
                                       List<Map<Long, List<Integer>>> corrTable) {
        List<Long> line = lines.get(curLine);
        // for every element of line:
        for (int j = 0; j < line.size(); j++) {
            Long element = line.get(j);
            // get connections to other lines
            Map<Long, List<Integer>> column = corrTable.get(j);
            List<Integer> linesToAdd = column.get(element);
            if (linesToAdd == null) {
                continue;
            }
            // add connected line to group and to queue
            //  if group did not already contain this line in it
            for (Integer lineToAdd : linesToAdd) {
                if (currentGroup.add(lineToAdd)) {
                    linesToCheck.add(lineToAdd);
                    lineGroups.set(lineToAdd, groupNumber);
                }
            }
        }
    }

    public static void writeResultsToFile(String inputFileName, String outputFileName, List<Long> linesOffsets, List<Set<Integer>> groups) {
        // create writer
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
            RandomAccessFile reader = new RandomAccessFile(inputFileName, "r")) {
            // write number of groups with more than one element
            for (int i = 0; i < groups.size(); i++) {
                if (groups.get(i).size() > 1) {
                    writer.write("Групп с более чем одним элементом: " + (groups.size() - i));
                    writer.newLine();
                    break;
                }
            }
            // write down all groups
            for (int groupNumber = groups.size() - 1; groupNumber >= 0; groupNumber--) {
                writer.write("Группа № " + (groups.size() - groupNumber));
                writer.newLine();
                for (int lineNumber : groups.get(groupNumber)) {
                    reader.seek(linesOffsets.get(lineNumber));
                    String line = reader.readLine();
                    writer.write(line);
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }
}
