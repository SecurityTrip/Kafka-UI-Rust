(function (window) {
  var RECONNECT_DELAY_MS = 2000;

  function connect() {
    var state = window.KafkaUIState;
    var render = window.KafkaUIRender;

    if (state.eventSource) {
      state.eventSource.close();
    }

    render.setStatus("Connecting live stream...", false);
    state.eventSource = new EventSource("/api/stream");

    state.eventSource.addEventListener("open", function () {
      render.setStatus("Live stream connected", false);
    });

    state.eventSource.addEventListener("snapshot", function (event) {
      try {
        var snapshot = JSON.parse(event.data);
        render.renderSnapshot(snapshot);
        var updatedAt = new Date().toLocaleTimeString();
        render.setStatus("Live stream connected | Last update " + updatedAt, false);
      } catch (_error) {
        render.setStatus("Failed to parse live update", true);
      }
    });

    state.eventSource.addEventListener("error", function () {
      render.setStatus("Live stream disconnected, reconnecting...", true);
      state.eventSource.close();

      if (state.reconnectTimer) {
        clearTimeout(state.reconnectTimer);
      }

      state.reconnectTimer = setTimeout(function () {
        connect();
      }, RECONNECT_DELAY_MS);
    });
  }

  window.KafkaUIStream = {
    connect: connect,
  };
})(window);
