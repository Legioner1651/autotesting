package ru.sberbank.sberuser.surms.test.integration;

import org.springframework.beans.factory.annotation.Value;
import org.testcontainers.utility.DockerImageName;

public interface ConfigEnv {
    public static final DockerImageName SURMS_DS_IMAGE = DockerImageName
            .parse(System.getenv("SURMS_DS_IMAGE")).asCompatibleSubstituteFor("postgres");

    public static final DockerImageName KAFKA_IMAGE = DockerImageName
            .parse(System.getenv("KAFKA_IMAGE")).asCompatibleSubstituteFor("apache/kafka");

    public static final DockerImageName APPLY_TASK_MANAGER_IMAGE = DockerImageName
            .parse(System.getenv("APPLY_TASK_MANAGER_IMAGE"));
    public static final String APPLY_TASK_MANAGER_USER = System.getenv("APPLY_TASK_MANAGER_USER");
    public static final String APPLY_TASK_MANAGER_PASSWORD = System.getenv("APPLY_TASK_MANAGER_PASSWORD");

    public static final DockerImageName SUDIR_SCIM_PROVIDER_IMAGE = DockerImageName
            .parse(System.getenv("SUDIR_SCIM_PROVIDER_IMAGE"));
}
