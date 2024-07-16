package main.java;

import org.checkerframework.checker.units.qual.C;

import java.sql.*;

public class Summer_practic {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "summer_session";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "password";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getSites(connection); System.out.println();
            getTitles(connection); System.out.println();
            getTranslators(connection); System.out.println();

            // TODO show with param
            getTitlesByCountry(connection, "Корея"); System.out.println();
            getTitlesByTranslator(connection, "Sky team"); System.out.println();
            getTitlesBetween(connection, 200, 2000); System.out.println();

            // TODO correction
            addTitle(connection, 3, "The King`s Avatar", "China", "Finished", 16, "Sky team", 1729); System.out.println();
            correctTitle(connection, "Выше богов", "Ongoing"); System.out.println();
            removeTitle(connection, "The King`s Avatar"); System.out.println();
            addTranslator(connection, "Phoenix team", 1, 2); System.out.println();


        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // region // Проверка окружения и доступа к базе данных

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    // endregion

    // region // SELECT-запросы без параметров в одной таблице

    private static void getSites(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "id", columnName1 = "name";
        // значения ячеек
        int param0 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM web_sites;"); // выполняем запроса на поиск и получаем список ответов

        System.out.println(columnName0 + " | " + columnName1);
        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param1 = rs.getString(columnName1); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1);
        }
    }

    static void getTitles(Connection connection) throws SQLException {
        String
                columnName0 = "title_id",
                columnName1 = "site_id",
                columnName2 = "name",
                columnName3 = "country",
                columnName4 = "status",
                columnName5 = "age_limit",
                columnName6 = "translator",
                columnName7 = "chapters_count";
        // значения ячеек
        int param0 = -1, param1 = -1, param5 = -1, param7 = -1;
        String param2 = null, param3 = null, param4 = null, param6 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM titles  ORDER BY title_id;");  // выполняем запроса на поиск и получаем список ответов

        System.out.println(columnName0 + " | " + columnName1 + " | " + columnName2 + " | " + columnName3 + " | " + columnName4 + " | " + columnName5 + " | " + columnName6 + " | " + columnName7);
        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(columnName0);
            param1 = rs.getInt(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            param5 = rs.getInt(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getInt(columnName7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    static void getTranslators(Connection connection) throws SQLException {
        String
                columnName0 = "id",
                columnName1 = "name",
                columnName2 = "site_id",
                columnName3 = "total_titles";
        String param1 = null;
        int param0 = -1, param2 = -1, param3 = -1;

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM translators;");   // выполняем запроса на поиск и получаем список ответов

        System.out.println(columnName0 + " | " + columnName1 + " | " + columnName2 + " | " + columnName3);
        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getInt(columnName2);
            param3 = rs.getInt(columnName3);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

    // endregion

    // region // SELECT-запросы с параметрами

    private static void getTitlesByCountry(Connection connection, String country) throws SQLException {
        if (country == null || country.isBlank()) return; // проверка "на дурака"

        Statement statement = connection.createStatement(); // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM titles  ORDER BY title_id;");
        System.out.println("Title(s) published in " + country);
        while (rs.next()) {
            if (rs.getString(4).contains(country)) {
                System.out.println(
                        rs.getInt(1) + " | " +
                                rs.getInt(2) + " | " +
                                rs.getString(3) + " | " +
                                rs.getString(4) + " | " +
                                rs.getString(5) + " | " +
                                rs.getInt(6) + " | " +
                                rs.getString(7) + " | " +
                                rs.getInt(8)
                );
            }
        }
    }

    private static void getTitlesByTranslator(Connection connection, String translator) throws SQLException {
        if (translator == null || translator.isBlank()) return; // проверка "на дурака"

        Statement statement = connection.createStatement(); // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM titles ORDER BY title_id;");
        System.out.println("Title(s) translated by " + translator);
        while (rs.next()) {
            if (rs.getString(7).contains(translator)) {
                System.out.println(
                        rs.getInt(1) + " | " +
                                rs.getInt(2) + " | " +
                                rs.getString(3) + " | " +
                                rs.getString(4) + " | " +
                                rs.getString(5) + " | " +
                                rs.getInt(6) + " | " +
                                rs.getString(7) + " | " +
                                rs.getInt(8)
                );
            }
        }
    }

    private static void getTitlesBetween(Connection connection, int low_bound, int high_bound) throws SQLException {
        if (low_bound < 0 || high_bound < 0) return;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM titles WHERE chapters_count BETWEEN " + low_bound + " AND " + high_bound + " ORDER BY title_id;");
        System.out.println("Titles between 200 and 2000 chapters:");
        while (rs.next()) {
            System.out.println(
                    rs.getInt(1) + " | " +
                            rs.getInt(2) + " | " +
                            rs.getString(3) + " | " +
                            rs.getString(4) + " | " +
                            rs.getString(5) + " | " +
                            rs.getInt(6) + " | " +
                            rs.getString(7) + " | " +
                            rs.getInt(8)
            );
        }
    }

    // endregion

    // region // CUD-запросы на добавление, изменение и удаление записей

    private static void addTitle(Connection connection, int site_id, String name, String country, String status, int age_limit, String translator, int chapters_count) throws SQLException {
        if (
                site_id < 0 ||
                        name == null || name.isBlank() ||
                        country == null || country.isBlank() ||
                        status == null || status.isBlank() ||
                        age_limit < 0 ||
                        translator == null || translator.isBlank() ||
                        chapters_count < 0
        ) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO titles (site_id, name, country, status, age_limit, translator, chapters_count) VALUES (?, ?, ?, ?, ?, ?, ?);",
                Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, site_id);
        statement.setString(2, name);    // "безопасное" добавление имени
        statement.setString(3, country);
        statement.setString(4, status);
        statement.setInt(5, age_limit);
        statement.setString(6, translator);
        statement.setInt(7, chapters_count);

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор нового тайтла " + rs.getInt(1));
        }

        System.out.println("Inserted " + count + " title");
        getTitles(connection);
    }

    private static void correctTitle(Connection connection, String name, String status) throws SQLException {
        if (name == null || name.isBlank() || status == null || status.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE titles SET status=? WHERE name=?;");
        statement.setString(1, status); // сначала что передаем
        statement.setString(2, name);   // затем по чему ищем

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("Updated " + count + " title(s)");
        getTitles(connection);
    }

    private static void removeTitle(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from titles WHERE name=?;");
        statement.setString(1, name);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("Removed " + count + " title(s)");
        getTitles(connection);
    }

    private static void addTranslator(Connection connection, String name, int site_id, int total_titles) throws SQLException {
        if (name == null || name.isBlank() || site_id < 0 || total_titles < 0) return;

        PreparedStatement statement = connection.prepareStatement("INSERT INTO translators (name, site_id, total_titles) VALUES (?, ?, ?);", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setInt(2, site_id);
        statement.setInt(3, total_titles);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор нвоой команды " + rs.getInt(1));
        }
        System.out.println("Inserted " + count + " translator");
        getTranslators(connection);
    }

    // endregion


}
