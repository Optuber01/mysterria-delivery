package net.mysterria.delivery.manager;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import java.io.StringReader;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class CompletionHistoryTest {
    @Test void partialAndDeliveredOutcomesSurviveJsonRoundTrip() {
        var states=Map.of("done",CompletionHistory.State.DELIVERED,"partial",CompletionHistory.State.PARTIAL);
        var loaded=CompletionHistory.read(new StringReader(new Gson().toJson(states)));
        assertTrue(CompletionHistory.response("done",loaded.get("done")).isSuccess());
        assertFalse(CompletionHistory.response("partial",loaded.get("partial")).isSuccess());
        assertNull(CompletionHistory.response("done",loaded.get("done")).getDeliveredAt());
    }
    @Test void legacyIdsBlockReplayWithoutInventingFullSuccess() {
        var loaded=CompletionHistory.read(new StringReader("[\"old-id\"]"));
        assertEquals(CompletionHistory.State.LEGACY,loaded.get("old-id"));
        assertFalse(CompletionHistory.response("old-id",loaded.get("old-id")).isSuccess());
    }
    @Test void corruptOrUnknownStateCannotBecomeSuccessfulCompletion() {
        for(String json:new String[]{"null","{\"id\":true}","{\"id\":\"UNKNOWN\"}","[null]","[\"\"]"}) {
            assertThrows(RuntimeException.class,()->CompletionHistory.read(new StringReader(json)),json);
        }
    }
}
