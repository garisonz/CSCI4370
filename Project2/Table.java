
/** **************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 *
 * compile javac --enable-preview --release 21 *.java
 * run     java --enable-preview MovieDB
 */
import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.StringTemplate.STR;
import static java.lang.System.arraycopy;
import static java.lang.System.out;

/**
 * **************************************************************************************
 * The Table class implements relational database tables (including attribute
 * names, domains and a list of tuples. Five basic relational algebra operators
 * are provided: project, select, union, minus and join. The insert data
 * manipulation operator is also provided. Missing are update and delete data
 * manipulation operators.
 */
public class Table implements Serializable {

    /**
     * Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /**
     * Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /**
     * Counter for naming temporary tables.
     */
    private static int count = 0;

    /**
     * Table name.
     */
    private final String name;

    /**
     * Array of attribute names.
     */
    private final String[] attribute;

    /**
     * Array of attribute domains: a domain may be integer types: Long, Integer,
     * Short, Byte real types: Double, Float string types: Character, String
     */
    private final Class[] domain;

    /**
     * Collection of tuples (data storage).
     */
    private final List<Comparable[]> tuples;

    /**
     * Primary key (the attributes forming).
     */
    private final String[] key;

    /**
     * Index into tuples (maps key to tuple).
     */
    private final Map<KeyType, Comparable[]> index;

    /**
     * The supported map types.
     */
    private enum MapType {
        NO_MAP, TREE_MAP, HASH_MAP, LINHASH_MAP, BPTREE_MAP
    }

    /**
     * The map type to be used for indices. Change as needed.
     */
    private static final MapType mType = MapType.LINHASH_MAP;

    /**
     * **********************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map<KeyType, Comparable[]> makeMap() {
        return switch (mType) {
            case NO_MAP ->
                null;
            case TREE_MAP ->
                new TreeMap<>();
            case HASH_MAP ->
                new HashMap<>();
            case LINHASH_MAP -> new LinHashMap <> (KeyType.class, Comparable [].class);
            //case BPTREE_MAP  -> new BpTreeMap <> (KeyType.class, Comparable [].class);
            default ->
                null;
        }; // switch
    } // makeMap


    /**
     * **********************************************************************************
     * Concatenate two arrays of type T to form a new wider array.
     *
     * @see
     * http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java
     *
     * @param arr1 the first array
     * @param arr2 the second array
     * @return a wider array containing all the values from arr1 and arr2
     */
    public static <T> T[] concat(T[] arr1, T[] arr2) {
        T[] result = Arrays.copyOf(arr1, arr1.length + arr2.length);
        arraycopy(arr2, 0, result, arr1.length, arr2.length);
        return result;
    } // concat

    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------
    /**
     * **********************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name the name of the relation
     * @param _attribute the string containing attributes names
     * @param _domain the string containing attribute domains (data types)
     * @param _key the primary key
     */
    public Table(String _name, String[] _attribute, Class[] _domain, String[] _key) {
        name = _name;
        attribute = _attribute;
        domain = _domain;
        key = _key;
        tuples = new ArrayList<>();
        index = makeMap();
        out.println(Arrays.toString(domain));
    } // constructor

    /**
     * **********************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples
     * list.
     *
     * @param _name the name of the relation
     * @param _attribute the string containing attributes names
     * @param _domain the string containing attribute domains (data types)
     * @param _key the primary key
     * @param _tuples the list of tuples containing the data
     */
    public Table(String _name, String[] _attribute, Class[] _domain, String[] _key,
            List<Comparable[]> _tuples) {
        name = _name;
        attribute = _attribute;
        domain = _domain;
        key = _key;
        tuples = _tuples;
        index = makeMap();
    } // constructor

    /**
     * **********************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name the name of the relation
     * @param attributes the string containing attributes names
     * @param domains the string containing attribute domains (data types)
     * @param _key the primary key
     */
    public Table(String _name, String attributes, String domains, String _key) {
        this(_name, attributes.split(" "), findClass(domains.split(" ")), _key.split(" "));

        out.println(STR."DDL> create table \{name} (\{attributes})");
    } // constructor

    /**
     * Create a unique index on the specified attribute.
     *
     * @param data The list of tuples to index.
     * @param attribute The attribute to index on.
     * @return The created index as a LinHashMap.
     */
    public LinHashMap<KeyType, Comparable[]> create_index(List<Comparable[]> data, String attribute) {

        LinHashMap<KeyType, Comparable[]> index = new LinHashMap<>(KeyType.class, Comparable[].class);  // Initialize the LinHashMap
        List<Comparable[]> duplicates = new ArrayList<>();  // To store duplicates

        int colPos = col(attribute);  // Find the column position for the attribute

        if (colPos == -1) {
            throw new IllegalArgumentException("Attribute not found: " + attribute);
        }

        for (Comparable[] tuple : data) {
            Comparable keyVal = tuple[colPos];
            KeyType key = new KeyType(new Comparable[]{keyVal});

            if (index.get(key) != null) {
                duplicates.add(tuple);  // Add to duplicates if key already exists
            } else {
                index.put(key, tuple);  // Add to the index if key is unique
            }
        }

        if (!duplicates.isEmpty()) {
            System.out.println("Duplicate keys found: " + duplicates);
            // Optionally remove duplicates from the data
            data.removeAll(duplicates);
        }

        return index;  // Return the created index
    }

    /**
     * Drop the primary index by clearing its contents and resetting it to null.
     */
    public void drop_index() {
        if (index != null) {
            index.clear();  // Clear the contents of the index
            System.out.println("Index dropped successfully.");
        } else {
            System.out.println("No index to drop.");
        }
    }
    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------
    /**
     * **********************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given
     * attributes. Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes the attributes to project onto
     * @return a table of projected tuples
     */
    public Table project(String attributes) {
    out.println("RA> " + name + ".project (" + attributes + ")");
    var attrs = attributes.split(" ");
    var colDomain = extractDom(match(attrs), domain);
    var newKey = (Arrays.asList(attrs).containsAll(Arrays.asList(key))) ? key : attrs;

    Set<List<Comparable>> uniqueRows = new HashSet<>();
    List<Comparable[]> rows = new ArrayList<>();

    for (Comparable[] tuple : tuples) {
        List<Comparable> projectedTuple = new ArrayList<>();
        for (String attr : attrs) {
            int colIndex = col(attr);
            projectedTuple.add(tuple[colIndex]);
        }
        if (uniqueRows.add(projectedTuple)) {
            rows.add(projectedTuple.toArray(new Comparable[0]));
        }
    }

    return new Table(name + count++, attrs, colDomain, newKey, rows);
}

    /**
     * **********************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate the check condition for tuples
     * @return a table with tuples satisfying the predicate
     */
    public Table select(Predicate<Comparable[]> predicate) {
        out.println(STR."RA> \{name}.select (\{predicate})");

        return new Table(name + count++, attribute, domain, key,
                tuples.stream().filter(t -> predicate.test(t))
                        .collect(Collectors.toList()));
    } // select

    /**
     * **********************************************************************************
     * Select the tuples satisfying the given simple condition on
     * attributes/constants compared using an <op> ==, !=, <, <=, >, >=.
     *
     * #usage movie.select ("year == 1977")
     *
     * @param condition the check condition as a string for tuples
     * @return a table with tuples satisfying the condition
     */
    public Table select(String condition) {
        out.println(STR."RA> \{name}.select (\{condition})");

        List<Comparable[]> rows = new ArrayList<>();
        var token = condition.split(" ");
        var colNo = col(token[0]);
        for (var t : tuples) {
            if (satifies(t, colNo, token[1], token[2])) {
                rows.add(t);
            }
        } // for

        return new Table(name + count++, attribute, domain, key, rows);
    } // select

    /**
     * **********************************************************************************
     * Does tuple t satify the condition t[colNo] op value where op is ==, !=,
     * <, <=, >, >=?
     *
     * #usage satisfies (t, 1, "<", "1980")
     *
     * @param colNo the attribute's column number
     * @param op the comparison operator
     * @param value the value to compare with (must be converted, String ->
     * domain type)
     * @return whether the condition is satisfied
     */
    private boolean satifies(Comparable[] t, int colNo, String op, String value) {
        var t_A = t[colNo];
        out.println(STR."satisfies: \{t_A} \{op} \{value}");
        var valt = switch (domain[colNo].getSimpleName()) {      // type converted
            case "Byte" ->
                Byte.valueOf(value);
            case "Character" ->
                value.charAt(0);
            case "Double" ->
                Double.valueOf(value);
            case "Float" ->
                Float.valueOf(value);
            case "Integer" ->
                Integer.valueOf(value);
            case "Long" ->
                Long.valueOf(value);
            case "Short" ->
                Short.valueOf(value);
            case "String" ->
                value;
            default ->
                value;
        }; // switch
        var comp = t_A.compareTo(valt);

        return switch (op) {
            case "==" ->
                comp == 0;
            case "!=" ->
                comp != 0;
            case "<" ->
                comp < 0;
            case "<=" ->
                comp <= 0;
            case ">" ->
                comp > 0;
            case ">=" ->
                comp >= 0;
            default ->
                false;
        }; // switch
    } // satifies

    /**
     * **********************************************************************************
     * Select the tuples satisfying the given key predicate (key = value). Use
     * an index (Map) to retrieve the tuple with the given key value. INDEXED
     * SELECT algorithm.
     *
     * @param keyVal the given key value
     * @return a table with the tuple satisfying the key predicate
     */
    public Table select(KeyType keyVal) {
        out.println(STR."RA> \{name}.select (\{keyVal})");

        List<Comparable[]> rows = new ArrayList<>();

        // Use the index to retrieve the tuple
        Comparable[] tuple = index.get(keyVal);
        if (tuple != null) {
            rows.add(tuple);
        } else {
            System.out.println("No tuple found for key: " + keyVal);
        }

        return new Table(name + count++, attribute, domain, key, rows);
    } // select

    /**
     * **********************************************************************************
     * Union this table and table2. Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2 the rhs table in the union operation
     * @return a table representing the union
     */
    public Table union(Table table2) {
        out.println(STR."RA> \{name}.union (\{table2.name})");
        if (!compatible(table2)) {
            return null;
        }

        List<Comparable[]> rows = new ArrayList<>();
        // Add all tuples from this table
        rows.addAll(tuples);

        // Create an index for the union operation
        LinHashMap<KeyType, Comparable[]> unionIndex = new LinHashMap<>(KeyType.class, Comparable[].class);

        // Add tuples from this table to the index
        for (Comparable[] tuple : tuples) {
            KeyType key = new KeyType(tuple);
            unionIndex.put(key, tuple);
        }

        // Add tuples from table2 that are not in the index
        for (Comparable[] tuple : table2.tuples) {
            KeyType key = new KeyType(tuple);
            if (unionIndex.get(key) == null) {
                rows.add(tuple);
                unionIndex.put(key, tuple);
            }
        }

        return new Table(name + count++, attribute, domain, key, rows);
    } // union

    /**
     * **********************************************************************************
     * Take the difference of this table and table2. Check that the two tables
     * are compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2 The rhs table in the minus operation
     * @return a table representing the difference
     */
    public Table minus(Table table2) {
        out.println(STR."RA> \{name}.minus (\{table2.name})");
        if (!compatible(table2)) {
            return null;
        }

        List<Comparable[]> rows = new ArrayList<>();

        // Create an index for the table2 to check for existence
        LinHashMap<KeyType, Comparable[]> table2Index = new LinHashMap<>(KeyType.class, Comparable[].class);

        // Add tuples from table2 to the index
        for (Comparable[] tuple : table2.tuples) {
            KeyType key = new KeyType(tuple);
            table2Index.put(key, tuple);
        }

        // Add tuples from this table that are not in table2
        for (Comparable[] tuple : tuples) {
            KeyType key = new KeyType(tuple);
            if (table2Index.get(key) == null) {
                rows.add(tuple);
            }
        }
        return new Table(name + count++, attribute, domain, key, rows);
    } // minus

    /**
     * **********************************************************************************
     * Join this table and table2 by performing an "equi-join". Tuples from both
     * tables are compared requiring attributes1 to equal attributes2.
     * Disambiguate attribute names by appending "2" to the end of any duplicate
     * attribute name. Implement using a NESTED LOOP JOIN ALGORITHM.
     *
     * #usage movie.join ("studioName", "name", studio)
     *
     * @param attributes1 the attributes of this table to be compared (Foreign
     * Key)
     * @param attributes2 the attributes of table2 to be compared (Primary Key)
     * @param table2 the rhs table in the join operation
     * @return a table with tuples satisfying the equality predicate
     */
    public Table join(String attributes1, String attributes2, Table table2) {
        out.println(STR."RA> \{name}.join (\{attributes1}, \{attributes2}, \{table2.name})");

        var t_attrs = attributes1.split(" ");
        var u_attrs = attributes2.split(" ");
        var rows = new ArrayList<Comparable[]>();

        // Get the column positions for join attributes in both tables
        int[] t_cols = match(t_attrs);
        int[] u_cols = table2.match(u_attrs);

        // Create new attribute array with disambiguated names
        String[] newAttribute = disambiguateAttributes(this.attribute, table2.attribute);

        // Create new domain array
        Class[] newDomain = concat(domain, table2.domain);

        // Perform the equi-join using nested loop join algorithm
        for (Comparable[] t_tuple : tuples) {
            for (Comparable[] u_tuple : table2.tuples) {
                if (equalityCheck(t_tuple, u_tuple, t_cols, u_cols)) {
                    rows.add(concat(t_tuple, u_tuple));
                }
            }
        }

        return new Table(name + count++, newAttribute, newDomain, key, rows);
    }

    private boolean equalityCheck(Comparable[] t_tuple, Comparable[] u_tuple, int[] t_cols, int[] u_cols) {
        for (int i = 0; i < t_cols.length; i++) {
            if (!t_tuple[t_cols[i]].equals(u_tuple[u_cols[i]])) {
                return false;
            }
        }
        return true;
    }

    private String[] disambiguateAttributes(String[] attrs1, String[] attrs2) {
        String[] result = new String[attrs1.length + attrs2.length];
        System.arraycopy(attrs1, 0, result, 0, attrs1.length);

        for (int i = 0; i < attrs2.length; i++) {
            String attr = attrs2[i];
            if (Arrays.asList(attrs1).contains(attr)) {
                result[attrs1.length + i] = attr + "2";
            } else {
                result[attrs1.length + i] = attr;
            }
        }

        return result;
    }

    /**
     * **********************************************************************************
     * Join this table and table2 by performing a "theta-join". Tuples from both
     * tables are compared attribute1 <op> attribute2. Disambiguate attribute
     * names by appending "2" to the end of any duplicate attribute name.
     * Implement using a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioName == name", studio)
     *
     * @param condition the theta join condition
     * @param table2 the rhs table in the join operation
     * @return a table with tuples satisfying the condition
     */
    public Table join(String condition, Table table2) {
        out.println(STR."RA> \{name}.join (\{condition}, \{table2.name})");

        var rows = new ArrayList<Comparable[]>();

        // Parse the string condition into three parts
        String[] condParts = condition.split(" ");
        String attribute1 = condParts[0];
        String operator = condParts[1];
        String attribute2 = condParts[2];

        // Find the indices of the attributes in the respective tables
        int index1 = Arrays.asList(attribute).indexOf(attribute1);
        int index2 = Arrays.asList(table2.attribute).indexOf(attribute2);

        for (Comparable[] row1 : tuples) {
            for (Comparable[] row2 : table2.tuples) {
                // Compare the attributes based on the operator
                boolean match = false;
                int comp = row1[index1].compareTo(row2[index2]);

                switch (operator) {
                    case "==":
                        match = comp == 0;
                        break;
                    case "!=":
                        match = comp != 0;
                        break;
                    case "<":
                        match = comp < 0;
                        break;
                    case "<=":
                        match = comp <= 0;
                        break;
                    case ">":
                        match = comp > 0;
                        break;
                    case ">=":
                        match = comp >= 0;
                        break;
                }

                if (match) {
                    // Create a new tuple for the joined table
                    Comparable[] newRow = new Comparable[attribute.length + table2.attribute.length];
                    System.arraycopy(row1, 0, newRow, 0, row1.length);
                    System.arraycopy(row2, 0, newRow, row1.length, row2.length);
                    rows.add(newRow);
                }
            }
        }

        // Concatenate attributes and domains
        String[] newAttributes = concat(attribute, table2.attribute);
        Class[] newDomains = concat(domain, table2.domain);

        // Disambiguate attribute names
        for (int i = 0; i < attribute.length; i++) {
            for (int j = 0; j < table2.attribute.length; j++) {
                if (attribute[i].equals(table2.attribute[j])) {
                    newAttributes[j + attribute.length] += "2";
                }
            }
        }

        return new Table(name + count++, newAttributes, newDomains, key, rows);
    } // join

    /**
     * **********************************************************************************
     * Join this table and table2 by performing an "equi-join". Same as above
     * equi-join, but implemented using an INDEXED JOIN algorithm.
     *
     * @param attributes1 the attributes of this table to be compared (Foreign
     * Key)
     * @param attributes2 the attributes of table2 to be compared (Primary Key)
     * @param table2 the rhs table in the join operation
     * @return a table with tuples satisfying the equality predicate
     */
    public Table i_join(String attributes1, String attributes2, Table table2)
{
    out.println(STR."RA> \{name}.i_join (\{attributes1}, \{attributes2}, \{table2.name})");

    var t_attrs = attributes1.split(" ");
    var u_attrs = attributes2.split(" ");
    var rows = new ArrayList<Comparable[]>();

    int[] t_cols = match(t_attrs);
    int[] u_cols = table2.match(u_attrs);

    for (Comparable[] t : tuples) {
        KeyType key = new KeyType(extract(t, t_attrs));
        Comparable[] u = table2.index.get(key);
        if (u != null) {
            rows.add(concat(t, u));
        }
    }

    return new Table(name + count++, concat(attribute, table2.attribute),
                     concat(domain, table2.domain), key, rows);
}

    /**
     * **********************************************************************************
     * Join this table and table2 by performing a NATURAL JOIN. Tuples from both
     * tables are compared, requiring common attributes to be equal. The
     * duplicate column is also eliminated.
     *
     * #usage movieStar.join(starsIn)
     *
     * @param table2 the rhs table in the join operation
     * @return a table with tuples satisfying the equality predicate
     */
    public Table join(Table table2) {
        out.println(STR."RA> \{name}.join (\{table2.name})");

        var rows = new ArrayList<Comparable[]>();

        // Find common attributes
        List<String> commonAttrs = Arrays.stream(attribute).filter(attr -> table2.col(attr) != -1)
                                        .collect(Collectors.toList());
        // Get the positions of the common attributes in both tables
        int[] pos1 = match(commonAttrs.toArray(new String[0]));
        int[] pos2 = table2.match(commonAttrs.toArray(new String[0]));

        

        // Perform the join operation
        for (var tup1 : this.tuples) {
            for (var tup2 : table2.tuples) {
                boolean match = IntStream.range(0, pos1.length)
                        .allMatch(i -> tup1[pos1[i]].equals(tup2[pos2[i]]));
                if (match) {
                    Comparable[] combinedTuple = new Comparable[this.attribute.length + table2.attribute.length - commonAttrs.size()];
                    int index = 0;
                    for (Comparable value : tup1) {
                        combinedTuple[index++] = value;
                    }
                    for (int j = 0; j < table2.attribute.length; j++) {
                        if (!commonAttrs.contains(table2.attribute[j])) {
                            combinedTuple[index++] = tup2[j];
                        }
                    }
                    rows.add(combinedTuple);
                }
            }
        }

        // Create the combined attribute and domain arrays
        String[] newAttributes = Stream.concat(Arrays.stream(this.attribute),
                Arrays.stream(table2.attribute)
                        .filter(attr -> !commonAttrs.contains(attr)))
                .toArray(String[]::new);
        Class[] newDomains = Stream.concat(Arrays.stream(this.domain),
                Arrays.stream(table2.domain)
                        .filter(d -> !commonAttrs.contains(table2.attribute[Arrays.asList(table2.domain).indexOf(d)])))
                .toArray(Class[]::new);

        return new Table(name + count++, newAttributes, newDomains, key, rows);

    } // join

    /**
     * **********************************************************************************
     * Return the column position for the given attribute name or -1 if not
     * found.
     *
     * @param attr the given attribute name
     * @return a column position
     */
    public int col(String attr) {
        for (var i = 0; i < attribute.length; i++) {
            if (attr.equals(attribute[i])) {
                return i;
            }
        } // for

        return -1;       // -1 => not found
    } // col

    /**
     * **********************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("Star_Wars", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup the array of attribute values forming the tuple
     * @return whether insertion was successful
     */
    public boolean insert(Comparable[] tup) {
        out.println(STR."DML> insert into \{name} values (\{Arrays.toString(tup)})");

        if (typeCheck(tup)) {
            tuples.add(tup);
            var keyVal = new Comparable[key.length];
            var cols = match(key);
            for (var j = 0; j < keyVal.length; j++) {
                keyVal[j] = tup[cols[j]];
            }
            if (mType != MapType.NO_MAP) {
                index.put(new KeyType(keyVal), tup);
            }
            return true;
        } else {
            return false;
        } // if
    } // insert

    /**
     * **********************************************************************************
     * Get the name of the table.
     *
     * @return the table's name
     */
    public String getName() {
        return name;
    } // getName

    /**
     * **********************************************************************************
     * Print this table.
     */
    public void print() {
        out.println(STR."\n Table \{name}");
        out.print("|-");
        out.print("---------------".repeat(attribute.length));
        out.println("-|");
        out.print("| ");
        for (var a : attribute) {
            out.printf("%15s", a);
        }
        out.println(" |");
        out.print("|-");
        out.print("---------------".repeat(attribute.length));
        out.println("-|");
        for (var tup : tuples) {
            out.print("| ");
            for (var attr : tup) {
                out.printf("%15s", attr);
            }
            out.println(" |");
        } // for
        out.print("|-");
        out.print("---------------".repeat(attribute.length));
        out.println("-|");
    } // print

    /**
     * **********************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex() {
        out.println(STR."\n Index for \{name}");
        out.println("-------------------");
        if (mType != MapType.NO_MAP) {
            for (var e : index.entrySet()) {
                out.println(STR."\{e.getKey()} -> \{Arrays.toString(e.getValue())}");
            } // for
        } // if
        out.println("-------------------");
    } // printIndex

    /**
     * **********************************************************************************
     * Load the table with the given name into memory.
     *
     * @param name the name of the table to load
     */
    public static Table load(String name) {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(DIR + name + EXT));
            tab = (Table) ois.readObject();
            ois.close();
        } catch (IOException ex) {
            out.println("load: IO Exception");
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            out.println("load: Class Not Found Exception");
            ex.printStackTrace();
        } // try
        return tab;
    } // load

    /**
     * **********************************************************************************
     * Save this table in a file.
     */
    
    public void save() {
        try {
            var oos = new ObjectOutputStream(new FileOutputStream(DIR + name + EXT));
            oos.writeObject(this);
            oos.close();
        } catch (IOException ex) {
            out.println("save: IO Exception");
            ex.printStackTrace();
        } // try
    } // save
    
    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------
    /**
     * **********************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e.,
     * have the same number of attributes each with the same corresponding
     * domain.
     *
     * @param table2 the rhs table
     * @return whether the two tables are compatible
     */
    private boolean compatible(Table table2) {
        if (domain.length != table2.domain.length) {
            out.println("compatible ERROR: table have different arity");
            return false;
        } // if
        for (var j = 0; j < domain.length; j++) {
            if (domain[j] != table2.domain[j]) {
                out.println(STR."compatible ERROR: tables disagree on domain \{j}");
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /**
     * **********************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column the array of column names
     * @return an array of column index positions
     */
    private int[] match(String[] column) {
        int[] colPos = new int[column.length];

        for (var j = 0; j < column.length; j++) {
            var matched = false;
            for (var k = 0; k < attribute.length; k++) {
                if (column[j].equals(attribute[k])) {
                    matched = true;
                    colPos[j] = k;
                } // for
            } // for
            if (!matched) {
                out.println(STR."match: domain not found for \{column[j]}");
            }
        } // for

        return colPos;
    } // match

    /**
     * **********************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t the tuple to extract from
     * @param column the array of column names
     * @return a smaller tuple extracted from tuple t
     */
    private Comparable[] extract(Comparable[] t, String[] column) {
        var tup = new Comparable[column.length];
        var colPos = match(column);
        for (var j = 0; j < column.length; j++) {
            tup[j] = t[colPos[j]];
        }
        return tup;
    } // extract

    /**
     * **********************************************************************************
     * Check the size of the tuple (number of elements in array) as well as the
     * type of each value to ensure it is from the right domain.
     *
     * @param t the tuple as a array of attribute values
     * @return whether the tuple has the right size and values that comply with
     * the given domains
     */
    private boolean typeCheck(Comparable[] t) {
        //  T O   B E   I M P L E M E N T E D 

        return true;      // change once implemented
    } // typeCheck

    /**
     * **********************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className the array of class name (e.g., {"Integer", "String"})
     * @return an array of Java classes
     */
    private static Class[] findClass(String[] className) {
        var classArray = new Class[className.length];

        for (var i = 0; i < className.length; i++) {
            try {
                classArray[i] = Class.forName(STR."java.lang.\{className[i]}");
            } catch (ClassNotFoundException ex) {
                out.println(STR."findClass: \{ex}");
            } // try
        } // for

        return classArray;
    } // findClass

    /**
     * **********************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group where to extract from
     * @return the extracted domains
     */
    private Class[] extractDom(int[] colPos, Class[] group) {
        var obj = new Class[colPos.length];

        for (var j = 0; j < colPos.length; j++) {
            obj[j] = group[colPos[j]];
        } // for

        return obj;
    } // extractDom

} // Table
