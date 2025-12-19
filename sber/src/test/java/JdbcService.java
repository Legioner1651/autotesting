package ru.sberbank.sberuser.surms.test.integration;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import io.qameta.allure.model.StatusDetails;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


@Slf4j
public class JdbcService {

//    @Autowired
//    private Param param;

    private final String dbUrl;
    private final String surmsDsUser;
    private final String surmsDsPassword;

    public JdbcService(String dbUrl, String surmsDsUser, String surmsDsPassword) {
        this.dbUrl = dbUrl;
        this.surmsDsUser = surmsDsUser;
        this.surmsDsPassword = surmsDsPassword;
    }

    @Step("Выполняем SQL-запрос на вставку данных в БД")
    public Integer callDbInsert(String sqlRequest) throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbUrl, surmsDsUser, surmsDsPassword);
             Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sqlRequest);
        } catch (SQLException e) {
            throw new SQLException("Ошибка выполнения SQL-запроса.", e);
        }
    }

    public String getSelect(String sqlRequest) throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbUrl, surmsDsUser, surmsDsPassword);
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sqlRequest);

            // Собираем результаты запроса в строку
            StringBuilder sb = new StringBuilder();
            while (resultSet.next()) {
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    if (i > 1) {
                        sb.append(", ");
                    }
                    sb.append(resultSet.getString(i));
                }
                sb.append("\n");
            }

            return sb.toString();
        } catch (SQLException e) {
            throw new SQLException("Ошибка выполнения SQL-запроса.", e);
        }
    }

    @Step("SQL-запрос: Проверка БД на наличие записей, на которые будет реагировать сервис")
    public String callDbSelectCheck(String sqlRequest) throws SQLException {
        String result = getSelect(sqlRequest);
        Allure.getLifecycle().updateStep(stepResult -> stepResult.setStatusDetails(new StatusDetails().setMessage(result)));
        return result;
    }

    @Step("Выполняем SQL-запрос на получение данных из БД")
    public String callDbSelect(String sqlRequest) throws SQLException {
        String result = getSelect(sqlRequest);
        Allure.getLifecycle().updateStep(stepResult -> stepResult.setStatusDetails(new StatusDetails().setMessage(result)));
        return result;
    }
}

