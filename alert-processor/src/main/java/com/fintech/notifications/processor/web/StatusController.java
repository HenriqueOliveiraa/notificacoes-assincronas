package com.fintech.notifications.processor.web;

import com.fintech.notifications.processor.service.StatusQueryService;
import com.fintech.notifications.processor.web.dto.AlertStatusResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/alerts")
@RequiredArgsConstructor
public class StatusController {

    private final StatusQueryService statusQueryService;

    @GetMapping("/{correlationId}/status")
    public AlertStatusResponse getStatus(@PathVariable UUID correlationId) {
        return statusQueryService.getStatus(correlationId.toString());
    }
}
