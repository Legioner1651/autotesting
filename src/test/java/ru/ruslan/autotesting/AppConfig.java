package ru.ruslan.autotesting;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({Param.class})
public class AppConfig {
}
