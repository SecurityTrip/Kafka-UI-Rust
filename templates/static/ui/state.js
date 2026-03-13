(function (window) {
  window.KafkaUIState = {
    activeView: "topics",
    latestSnapshot: null,
    selectedTopic: null,
    eventSource: null,
    reconnectTimer: null,
  };
})(window);
