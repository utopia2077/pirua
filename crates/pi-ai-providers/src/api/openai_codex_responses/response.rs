use super::*;
pub(super) enum Input {
    Http {
        body: Box<dyn ResponseBody>,
        decoder: Box<SseDecoder>,
        eof: bool,
    },
    Socket(Box<dyn WebSocketExchange>),
}
pub(super) struct Source {
    pub input: Input,
    pub runtime: RequestRuntime,
    pub template: AssistantMessage,
    pub producer: AssistantStreamProducer,
    pub started: bool,
    pub emit_start: bool,
    pub websocket: bool,
    pub finished: bool,
    pub failure: Option<Failure>,
    pub idle: Option<Duration>,
    pub max_message: usize,
}
impl Drop for Source {
    fn drop(&mut self) {
        if let Input::Http { body, .. } = &mut self.input {
            body.close();
        }
    }
}
impl Source {
    async fn wire(&mut self) -> Result<Option<Value>, Failure> {
        match &mut self.input {
            Input::Socket(exchange) => {
                let event = cancellable(
                    exchange.next(&self.runtime, self.idle),
                    &self.runtime.cancellation,
                )
                .await
                .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))??;
                match event {
                    Some(WebSocketEvent::Message(message)) => {
                        let text = match message {
                            WebSocketMessage::Text(text) => text,
                            WebSocketMessage::Binary(bytes) => String::from_utf8_lossy(&bytes)
                                .strip_prefix('\u{feff}')
                                .unwrap_or(&String::from_utf8_lossy(&bytes))
                                .to_owned(),
                        };
                        if text.len() > self.max_message {
                            return Err(Failure::Limit);
                        }
                        if text.is_empty() {
                            return Ok(Some(json!({})));
                        }
                        serde_json::from_str(&text).map(Some).map_err(|_| {
                            Failure::Protocol(classify(
                                "Invalid Codex WebSocket JSON",
                                &self.template,
                            ))
                        })
                    }
                    Some(WebSocketEvent::Close { code, reason }) => {
                        let text = format!(
                            "WebSocket closed {code} {}",
                            if reason.is_empty() && code == 1009 {
                                "message too big"
                            } else {
                                &reason
                            }
                        );
                        Err(Failure::TransportText(
                            classify(&text, &self.template),
                            code,
                        ))
                    }
                    None => Err(Failure::Transport(TransportErrorKind::Connection)),
                }
            }
            Input::Http { body, decoder, eof } => loop {
                if let Some(frame) = decoder.next_frame()? {
                    return serde_json::from_str(&frame.data).map(Some).map_err(|_| {
                        Failure::Protocol(classify("Invalid Codex SSE JSON", &self.template))
                    });
                }
                if *eof {
                    return Ok(None);
                }
                match cancellable(body.next(&self.runtime), &self.runtime.cancellation)
                    .await
                    .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))??
                {
                    Some(bytes) => decoder.push(&bytes)?,
                    None => {
                        *eof = true;
                        decoder.end_input();
                    }
                }
            },
        }
    }
    fn map(&mut self, mut event: Value) -> Result<Option<Value>, Failure> {
        if event.is_null() {
            return Err(Failure::Protocol(ResponsesError::new(
                ResponsesErrorKind::InvalidWire,
            )));
        }
        let Some(kind) = event
            .get("type")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        if kind == "error" || kind == "response.failed" {
            let nested = if kind == "error" {
                &event["error"]
            } else {
                &event["response"]["error"]
            };
            let code = if kind == "error" {
                event["code"].as_str().or_else(|| nested["code"].as_str())
            } else {
                nested["code"].as_str()
            };
            let message = if kind == "error" {
                event["message"]
                    .as_str()
                    .or_else(|| nested["message"].as_str())
            } else {
                nested["message"].as_str()
            };
            let text = if kind == "error" {
                format!(
                    "Codex error: {}",
                    message
                        .filter(|s| !s.is_empty())
                        .or(code.filter(|s| !s.is_empty()))
                        .map(str::to_owned)
                        .unwrap_or_else(|| event.to_string())
                )
            } else {
                message
                    .filter(|s| !s.is_empty())
                    .unwrap_or("Codex response failed")
                    .to_owned()
            };
            let code = match code {
                Some("websocket_connection_limit_reached") => ApiCode::ConnectionLimit,
                Some("previous_response_not_found") => ApiCode::PreviousMissing,
                _ => ApiCode::Other,
            };
            return Err(Failure::Api(code, classify(&text, &self.template)));
        }
        if matches!(
            kind,
            "response.done" | "response.completed" | "response.incomplete"
        ) {
            // The source mapper updates this metadata before its first-yield
            // callback publishes WS Start. Other fields still precede reduction.
            if self.websocket
                && !self.started
                && let Some(value) = event["response"]["end_turn"].as_bool()
            {
                self.template.end_turn = Some(value);
            }
            event["type"] = json!("response.completed");
            if let Some(response) = event.get_mut("response").and_then(Value::as_object_mut)
                && !response
                    .get("status")
                    .and_then(Value::as_str)
                    .is_some_and(|s| {
                        matches!(
                            s,
                            "completed"
                                | "incomplete"
                                | "failed"
                                | "cancelled"
                                | "queued"
                                | "in_progress"
                        )
                    })
            {
                response.shift_remove("status");
            }
            self.finished = true;
        }
        if self.websocket && !self.started {
            self.started = true;
            if self.emit_start {
                let _ = self.producer.push(AssistantMessageEvent::Start {
                    partial: Box::new(self.template.clone()),
                });
            }
        }
        Ok(Some(event))
    }
}
impl ResponsesEventSource for Source {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Value>, ResponsesError>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            loop {
                let result = match self.wire().await {
                    Ok(Some(event)) => self.map(event),
                    Ok(None) => return Ok(None),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(Some(event)) => return Ok(Some(event)),
                    Ok(None) => {}
                    Err(error) => {
                        let flags = error.flags();
                        self.failure = Some(error);
                        return Err(flags);
                    }
                }
            }
        })
    }
}
pub(super) struct Pricing<'a>(pub &'a Model);
impl ResponsesServiceTierPricing for Pricing<'_> {
    fn apply(
        &self,
        usage: &mut Usage,
        tier: Presence<ResponsesServiceTier>,
    ) -> Result<(), ResponsesError> {
        let factor = match tier {
            Presence::Value(ResponsesServiceTier::Flex) => 0.5,
            Presence::Value(ResponsesServiceTier::Priority) => {
                if self.0.id.as_str() == "gpt-5.5" {
                    2.5
                } else {
                    2.0
                }
            }
            _ => 1.0,
        };
        for cost in [
            &mut usage.cost.input,
            &mut usage.cost.output,
            &mut usage.cost.cache_read,
            &mut usage.cost.cache_write,
        ] {
            *cost = FiniteF64::new(cost.get() * factor)
                .map_err(|_| ResponsesError::new(ResponsesErrorKind::Policy))?;
        }
        usage.cost.total = FiniteF64::new(
            usage.cost.input.get()
                + usage.cost.output.get()
                + usage.cost.cache_read.get()
                + usage.cost.cache_write.get(),
        )
        .map_err(|_| ResponsesError::new(ResponsesErrorKind::Policy))?;
        Ok(())
    }
}
impl ResponsesServiceTierPolicy for Pricing<'_> {
    fn pricing(&self) -> Option<&dyn ResponsesServiceTierPricing> {
        Some(self)
    }
    fn resolve(
        &self,
        response: Presence<ResponsesServiceTier>,
        request: Presence<ResponsesServiceTier>,
    ) -> Presence<ResponsesServiceTier> {
        if matches!(response, Presence::Value(ResponsesServiceTier::Default))
            && matches!(
                request,
                Presence::Value(ResponsesServiceTier::Flex | ResponsesServiceTier::Priority)
            )
        {
            request
        } else {
            match response {
                Presence::Value(_) => response,
                _ => request,
            }
        }
    }
}
