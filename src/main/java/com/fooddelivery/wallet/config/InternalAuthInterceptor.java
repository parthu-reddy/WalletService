package com.fooddelivery.wallet.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import java.util.Set;

@Component
@Slf4j
public class InternalAuthInterceptor implements HandlerInterceptor {

    private static final Set<String> ALLOWED_SERVICES = Set.of(
            "PaymentGateway", 
            "CampaignService", 
            "BiddingEngine", 
            "BudgetLimitingService",
            "AdvertiserService" // if CampaignService calls it with this name
    );

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String callingService = request.getHeader("X-Calling-Service");
        if (callingService == null || !ALLOWED_SERVICES.contains(callingService)) {
            log.warn("Unauthorized internal access attempt by service {}", callingService);
            response.setStatus(HttpStatus.FORBIDDEN.value());
            return false;
        }
        return true;
    }
}
