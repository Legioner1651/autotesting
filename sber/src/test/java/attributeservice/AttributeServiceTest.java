package attributeservice;

import io.qameta.allure.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import ru.sberbank.sberuser.surms.test.integration.AbstractInstance;

import java.time.LocalTime;

@Slf4j
@Owner("Ruslan Petrov")
@Feature("Сервис SURMS.attributeService")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AttributeServiceTest extends AbstractInstance {

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        timeStart= LocalTime.now();
        headingOfTestMethod(testInfo, timeStart);
    }

    @AfterEach
    public void afterEach() {
        timeEnd= LocalTime.now();
        endOfTestMethod(timeStart, timeEnd);
    }

    @Test
    @Order(value = 1)
    @AllureId("7679616")
    @DisplayName("ATRS 01. Тестовый метод сервиса SURMS.attributeService")
    @Tags({@Tag("functional"), @Tag("get"), @Tag("positive"), @Tag("SURMS.attributeService"), @Tag("ATRS.01")})
    @Description("Тестовый метод 01")
    void methodTest1() {

        /* Параметры для ЦОИФТ */
        Allure.label("system", "CI03439028");
        Allure.label("team", "Ролевые модели [62868715]");
        Allure.label("story", "SURMS-0000");
        Allure.label("layer", "api");
        Allure.label("testStage", "ift");
        Allure.label("criticalRegress", "true");
        Allure.label("regress", "true");

        log.info("Тест 1");
    }

    @Test
    @Order(value = 2)
    @AllureId("7679612")
    @DisplayName("ATRS 02. Тестовый метод сервиса SURMS.attributeService")
    @Tags({@Tag("functional"), @Tag("get"), @Tag("positive"), @Tag("SURMS.attributeService"), @Tag("ATRS.02")})
    @Description("Тестовый метод 02")
    void methodTest2() {

        /* Параметры для ЦОИФТ */
        Allure.label("system", "CI03439028");
        Allure.label("team", "Ролевые модели [62868715]");
        Allure.label("story", "SURMS-0000");
        Allure.label("layer", "api");
        Allure.label("testStage", "ift");
        Allure.label("criticalRegress", "true");
        Allure.label("regress", "true");

        log.info("Тест 2");
    }

    @Test
    @Order(value = 3)
    @AllureId("7679602")
    @DisplayName("ATRS 03. Тестовый метод сервиса SURMS.attributeService")
    @Tags({@Tag("functional"), @Tag("get"), @Tag("positive"), @Tag("SURMS.attributeService"), @Tag("ATRS.03")})
    @Description("Тестовый метод 03")
    void methodTest3() {

        /* Параметры для ЦОИФТ */
        Allure.label("system", "CI03439028");
        Allure.label("team", "Ролевые модели [62868715]");
        Allure.label("story", "SURMS-0000");
        Allure.label("layer", "api");
        Allure.label("testStage", "ift");
        Allure.label("criticalRegress", "true");
        Allure.label("regress", "true");

        log.info("Тест 3");
    }

    @Test
    @Order(value = 4)
    @AllureId("7679598")
    @DisplayName("ATRS 04. Тестовый метод сервиса SURMS.attributeService")
    @Tags({@Tag("functional"), @Tag("get"), @Tag("positive"), @Tag("SURMS.attributeService"), @Tag("ATRS.04")})
    @Description("Тестовый метод 04")
    void methodTest4() {

        /* Параметры для ЦОИФТ */
        Allure.label("system", "CI03439028");
        Allure.label("team", "Ролевые модели [62868715]");
        Allure.label("story", "SURMS-0000");
        Allure.label("layer", "api");
        Allure.label("testStage", "ift");
        Allure.label("criticalRegress", "true");
        Allure.label("regress", "true");

        log.info("Тест 4");
    }

    @Test
    @Order(value = 5)
    @AllureId("7679605")
    @DisplayName("ATRS 05. Тестовый метод сервиса SURMS.attributeService")
    @Tags({@Tag("functional"), @Tag("get"), @Tag("positive"), @Tag("SURMS.attributeService"), @Tag("ATRS.05")})
    @Description("Тестовый метод 05")
    void methodTest5() {

        /* Параметры для ЦОИФТ */
        Allure.label("system", "CI03439028");
        Allure.label("team", "Ролевые модели [62868715]");
        Allure.label("story", "SURMS-0000");
        Allure.label("layer", "api");
        Allure.label("testStage", "ift");
        Allure.label("criticalRegress", "true");
        Allure.label("regress", "true");

        log.info("Тест 5");
    }
}

