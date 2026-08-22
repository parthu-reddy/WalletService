package com.fooddelivery.wallet.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    private final InternalAuthInterceptor internalAuthInterceptor;

    public WebMvcConfig(InternalAuthInterceptor internalAuthInterceptor) {
        this.internalAuthInterceptor = internalAuthInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(internalAuthInterceptor)
                .addPathPatterns("/api/v1/internal/**");
    }
}
