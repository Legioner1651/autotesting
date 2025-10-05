package ru.ruslan.autotesting;

import org.junit.jupiter.api.*;

public class ApplyTaskManagerTest {

    @Test
    public void testMethod() {
        System.out.println("*** ===  @Test  === Start === ***");
        String value1 = Param.getProperty("some.config.key1");
        System.out.println("some.config.key1: " + value1);
        String env = Param.getEnv("SSH_AUTH_SOCK");
        System.out.println("env SSH_AUTH_SOCK: " + env);
        System.out.println("*** ===  @Test  === End === ***");
    }
}

//    @BeforeAll
//    public static void beforeAll() {
//        System.out.println("=== @BeforeAll === Start ===");
//        // Вызов методов
//        GeneralMethods.printSomeConfigKey();
//        System.out.println("env SSH_AUTH_SOCK: " + Param.getEnv("SSH_AUTH_SOCK"));
//        System.out.println("=== @BeforeAll === End ===");
//    }
//
//    @BeforeEach
//    public void beforeEach() {
//        System.out.println("=== @BeforeEach === Start ===");
//        GeneralMethods.printSomeConfigKey();
//        System.out.println("env SSH_AUTH_SOCK: " + Param.getEnv("SSH_AUTH_SOCK"));
//        System.out.println("=== @BeforeEach === End ===");
//    }
//
//    @AfterEach
//    public void afterEach() {
//        System.out.println("=== @AfterEach === Start ===");
//        GeneralMethods.printSomeConfigKey();
//        System.out.println("env SSH_AUTH_SOCK: " + Param.getEnv("SSH_AUTH_SOCK"));
//        System.out.println("=== @AfterEach === End ===");
//    }
//
//    @AfterAll
//    public static void afterAll() {
//        System.out.println("=== @AfterAll === Start ===");
//        GeneralMethods.printSomeConfigKey();
//        System.out.println("env SSH_AUTH_SOCK: " + Param.getEnv("SSH_AUTH_SOCK"));
//        System.out.println("=== @AfterAll === End ===");
//    }
//}