(function (window) {
  window.KafkaUIState = {
    activeView: "topics",
    latestSnapshot: null,
    selectedTopic: null,
    topicOverviewTopic: null,
    topicOverview: null,
    topicOverviewLoading: false,
    topicOverviewError: "",
    topicMessagesTopic: null,
    topicMessages: [],
    selectedTopicMessageId: null,
    selectedPayloadTab: "value",
    topicMessagesLoading: false,
    topicMessagesError: "",
    topicDetailsRefreshTimer: null,
    selectedBroker: null,
    eventSource: null,
    reconnectTimer: null,
  };
})(window);
