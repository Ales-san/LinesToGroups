package org.example;

import java.io.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        Long startTime = System.currentTimeMillis();
        if (args.length < 1) {
            System.out.println("Не указано имя файла с исходными данными!");
            return;
        }

        // read all data and process it
        List<List<Double>> lines = new ArrayList<>();

        readAndProcessData(args[0], lines);
//        logTimeFromStart(startTime, "End of reading and processing data!");

        // get all information about same values in the same columns but from different lines
        List<Map<Integer, List<Integer>>> corrTable = new ArrayList<>();
        List<Integer> lineGroups = new ArrayList<>();
        getConnections(lines, lineGroups, corrTable);
//        logTimeFromStart(startTime, "End of parsing data and making table of connections!");

        // find all groups with lines with same values in the same columns
        List<Set<Integer>> groups = new ArrayList<>();
        findAllGroups(lines, lineGroups, groups, corrTable);
        // sort groups by size
        groups.sort(Comparator.comparingInt(Set::size));
//        logTimeFromStart(startTime, "End of calculating groups");

        // write down results of program
        writeResultsToFile("output.txt", lines, groups);
        logTimeFromStart(startTime, "End of program");
    }

    public static void logTimeFromStart(Long startTime, String event) {
//        System.out.println(event);
        Long endTime = System.currentTimeMillis();
        System.out.println("Time: " + (endTime - startTime) + " ms.");
    }

    public static void readAndProcessData(String inputFileName, List<List<Double>> lines) {
        // create reader
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFileName))) {
            String line;
            Set<String> setOfLines = new HashSet<>();
            // read line
            while ((line = reader.readLine()) != null) {
                // parse and write data in lines list
                processLine(line, lines, setOfLines);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void processLine(String line, List<List<Double>> lines, Set<String> setOfLines) {
        // check if the same line was before
        // this check could be more complex in case of different space symbols and any other problems
        //  line.matches(".*\\d.*") ->
        //      this for the way to not skip lines with empty values - as each line of this type should be in its own group
        if (line.isBlank()) { return; }
        if (setOfLines.contains(line) && line.matches(".*\\d.*")) {
            return;
        }
        // split elements by ";"
        List<String> elements = new ArrayList<>(Arrays.stream(line.split(";", -1)).toList());
        List<Double> parsedElements = new ArrayList<>(elements.size());
        if (elements.isEmpty()) {
            return;
        }
        // for every element:
        for (int i = 0; i < elements.size(); i++) {
            String element = elements.get(i);
            // check if element is in right format
            //  (regular expression was made on the basis of lng-big.csv)
            if (element.isBlank()) {
                parsedElements.add(null);
                continue;
            }
            if (!element.matches("\"[0-9]{0,13}(\\.[0-9]*)?\"")) {
                return;
            } else {
                // parse element and add to list
                String elWithoutQuotes = element.subSequence(1, element.length() - 1).toString();
                if (elWithoutQuotes.isBlank()) {
                    parsedElements.add(null);
                } else {
                    parsedElements.add(i, Double.parseDouble(elWithoutQuotes));
                }
            }
        }

        // add line hash code to set
        setOfLines.add(line);
        // add parsed line to list
        lines.add(parsedElements);
    }

    public static void getConnections(List<List<Double>> lines, List<Integer> lineGroups,
                                      List<Map<Integer, List<Integer>>> corrTable) {
        for (int i = 0; i < lines.size(); i++) {
            getConnection(i, lines, lineGroups, corrTable);
        }
    }

    public static void getConnection(int i, List<List<Double>> lines, List<Integer> lineGroups,
                                      List<Map<Integer, List<Integer>>> corrTable) {
            DecimalFormat format = new DecimalFormat("#.#####");
            // initialize group for every line with default value: -1
            lineGroups.add(-1);
            // for every column collect unique values and mark in which lines they are
            for (int j = 0; j < lines.get(i).size(); j++) {
                Double element = lines.get(i).get(j);
                // initialize map for column
                if (corrTable.size() < j + 1) {
                    corrTable.add(new HashMap<>());
                }
                // skip empty values
                if (element == null) {
                    continue;
                }
                // put element and number of line to corresponding map element
                Map<Integer, List<Integer>> column = corrTable.get(j);
                Integer key = format.format(element).hashCode();
                if (!column.containsKey(key)) {
                    column.put(key, new ArrayList<>(List.of(i)));
                } else {
                    List<Integer> columnGroup = column.get(key);
                    columnGroup.add(i);
                }
            }

    }

    public static void findAllGroups(List<List<Double>> lines, List<Integer> lineGroups,
                                     List<Set<Integer>> groups, List<Map<Integer, List<Integer>>> corrTable) {
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
                                       List<List<Double>> lines, List<Integer> lineGroups,
                                       List<Map<Integer, List<Integer>>> corrTable) {
        DecimalFormat format = new DecimalFormat("#.#####");
        List<Double> line = lines.get(curLine);
        // for every element of line:
        for (int j = 0; j < line.size(); j++) {
            Double element = line.get(j);
            if (element == null) {
                continue;
            }
            Integer key = format.format(element).hashCode();
            // get connections to other lines
            Map<Integer, List<Integer>> column = corrTable.get(j);
            List<Integer> linesToAdd = column.get(key);
            List<Integer> newColumnValues = new ArrayList<>();

            if (linesToAdd == null) {
                continue;
            }
            // add connected line to group and to queue
            //  if group did not already contain this line in it
            for (Integer lineToAdd : linesToAdd) {
                if (!currentGroup.contains(lineToAdd)) {
                    if (compare(lines.get(lineToAdd).get(j), element) == 0) {
                        currentGroup.add(lineToAdd);
                        linesToCheck.add(lineToAdd);
                        lineGroups.set(lineToAdd, groupNumber);
                    } else {
                        newColumnValues.add(lineToAdd);
                    }
                }
            }
            column.put(key, newColumnValues);
        }
    }

    public static void writeResultsToFile(String outputFileName, List<List<Double>> lines, List<Set<Integer>> groups) {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setDecimalSeparator('.');
        DecimalFormat format = new DecimalFormat("#.#####", symbols);
        // create writer
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName))) {
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
                    List<Double> line = lines.get(lineNumber);
                    for (int i = 0; i < line.size(); i++) {
                        Double el = line.get(i);
                        if (el != null) {
                            writer.write("\"" + format.format(el) + "\"");
                        } /*else {
                            writer.write("\"\"");
                        }*/
                        if (i < line.size() - 1) {
                            writer.write(";");
                        }
                    }
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

//    public static class DoubleComparator implements Comparator<Double> {

        public static int compare(Double o1, Double o2) {
            double delta = o1 - o2;
            if (Math.abs(delta) < 1E-5) {
                return 0;
            }
            if (delta < 0) {
                return -1;
            }
            return 1;
        }
//    }
}
