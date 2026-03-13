(function (window) {
  window.KafkaUIState = {
    activeView: "topics",
    latestSnapshot: null,
    selectedTopic: null,
    selectedBroker: null,
    eventSource: null,
    reconnectTimer: null,
  };
})(window);
