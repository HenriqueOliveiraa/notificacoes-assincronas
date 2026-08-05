package com.fintech.notifications.api.web;

import com.fintech.notifications.api.service.AlertDispatchService;
import com.fintech.notifications.api.web.dto.DispatchAlertRequest;
import com.fintech.notifications.api.web.dto.DispatchAlertResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/channels/{channelId}/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final AlertDispatchService dispatchService;

    @PostMapping
    public ResponseEntity<DispatchAlertResponse> dispatch(@PathVariable String channelId,
                                                          @Valid @RequestBody DispatchAlertRequest request) {
        UUID correlationId = dispatchService.dispatch(channelId, request);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(DispatchAlertResponse.accepted(correlationId));
    }
}
