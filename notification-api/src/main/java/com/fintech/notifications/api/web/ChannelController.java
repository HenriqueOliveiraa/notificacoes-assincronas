package com.fintech.notifications.api.web;

import com.fintech.notifications.api.service.ChannelService;
import com.fintech.notifications.api.web.dto.ChannelRequest;
import com.fintech.notifications.api.web.dto.ChannelResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/channels")
@RequiredArgsConstructor
public class ChannelController {

    private final ChannelService channelService;

    @PostMapping
    public ResponseEntity<ChannelResponse> create(@Valid @RequestBody ChannelRequest request,
                                                  UriComponentsBuilder uriBuilder) {
        ChannelResponse body = ChannelResponse.from(channelService.create(request));
        URI location = uriBuilder.path("/channels/{id}").buildAndExpand(body.id()).toUri();
        return ResponseEntity.created(location).body(body);
    }

    @GetMapping
    public List<ChannelResponse> list() {
        return channelService.findAll().stream().map(ChannelResponse::from).toList();
    }

    @GetMapping("/{id}")
    public ChannelResponse getById(@PathVariable String id) {
        return ChannelResponse.from(channelService.findById(id));
    }

    @PutMapping("/{id}")
    public ChannelResponse update(@PathVariable String id, @Valid @RequestBody ChannelRequest request) {
        return ChannelResponse.from(channelService.update(id, request));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable String id) {
        channelService.delete(id);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
