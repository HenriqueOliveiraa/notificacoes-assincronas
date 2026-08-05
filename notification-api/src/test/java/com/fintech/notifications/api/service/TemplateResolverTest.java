package com.fintech.notifications.api.service;

import com.fintech.notifications.api.exception.TemplateResolutionException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TemplateResolverTest {

    private final TemplateResolver resolver = new TemplateResolver();

    @Test
    void resolvesAllPlaceholders() {
        String template = "Olá {{clientName}}, sua fatura de {{billingMonth}} está disponível.";
        Map<String, Object> params = Map.of(
                "clientName", "João Silva",
                "billingMonth", "Novembro/2025");

        String result = resolver.resolve(template, params);

        assertThat(result).isEqualTo("Olá João Silva, sua fatura de Novembro/2025 está disponível.");
    }

    @Test
    void handlesWhitespaceInsidePlaceholders() {
        String result = resolver.resolve("Valor: {{ amount }}", Map.of("amount", 42));
        assertThat(result).isEqualTo("Valor: 42");
    }

    @Test
    void throwsWhenParamsMissing() {
        String template = "Olá {{clientName}}, mês {{billingMonth}}";

        assertThatThrownBy(() -> resolver.resolve(template, Map.of("clientName", "Ana")))
                .isInstanceOf(TemplateResolutionException.class)
                .satisfies(ex -> assertThat(((TemplateResolutionException) ex).getMissingParams())
                        .containsExactly("billingMonth"));
    }

    @Test
    void reportsAllMissingParamsAtOnce() {
        assertThatThrownBy(() -> resolver.resolve("{{a}}-{{b}}", Map.of()))
                .isInstanceOf(TemplateResolutionException.class)
                .satisfies(ex -> assertThat(((TemplateResolutionException) ex).getMissingParams())
                        .containsExactlyInAnyOrder("a", "b"));
    }
}
