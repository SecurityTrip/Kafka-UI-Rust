(function (window) {
  var refs = {
    liveStatus: document.getElementById("live-status"),
    bootstrapServers: document.getElementById("bootstrap-servers"),
    clusterId: document.getElementById("cluster-id"),
    controllerId: document.getElementById("controller-id"),
    brokerCount: document.getElementById("broker-count"),
    topicCount: document.getElementById("topic-count"),
    groupCount: document.getElementById("group-count"),
    brokersTitle: document.getElementById("brokers-title"),
    topicsTitle: document.getElementById("topics-title"),
    groupsTitle: document.getElementById("groups-title"),
    brokersBody: document.getElementById("brokers-body"),
    topicsBody: document.getElementById("topics-body"),
    groupsBody: document.getElementById("groups-body"),
    topicsSearch: document.getElementById("topics-search"),
    groupsSearch: document.getElementById("groups-search"),
    detailsEmpty: document.getElementById("topic-details-empty"),
    detailsGrid: document.getElementById("topic-details-grid"),
    partitionsWrap: document.getElementById("topic-partitions-wrap"),
    detailTopicName: document.getElementById("detail-topic-name"),
    detailTopicPartitions: document.getElementById("detail-topic-partitions"),
    detailTopicReplication: document.getElementById("detail-topic-replication"),
    detailTopicInternal: document.getElementById("detail-topic-internal"),
    topicPartitionsBody: document.getElementById("topic-partitions-body"),
    navItems: document.querySelectorAll(".nav-item[data-view]"),
    overviewPanel: document.getElementById("overview-panel"),
    topicsPanel: document.getElementById("topics-panel"),
    topicDetailsPanel: document.getElementById("topic-details-panel"),
    brokersPanel: document.getElementById("brokers-panel"),
    groupsPanel: document.getElementById("groups-panel"),
  };

  function isReady() {
    return (
      refs.liveStatus &&
      refs.bootstrapServers &&
      refs.clusterId &&
      refs.controllerId &&
      refs.brokerCount &&
      refs.topicCount &&
      refs.groupCount &&
      refs.brokersTitle &&
      refs.topicsTitle &&
      refs.groupsTitle &&
      refs.brokersBody &&
      refs.topicsBody &&
      refs.groupsBody &&
      refs.topicsSearch &&
      refs.groupsSearch &&
      refs.detailsEmpty &&
      refs.detailsGrid &&
      refs.partitionsWrap &&
      refs.detailTopicName &&
      refs.detailTopicPartitions &&
      refs.detailTopicReplication &&
      refs.detailTopicInternal &&
      refs.topicPartitionsBody &&
      refs.overviewPanel &&
      refs.topicsPanel &&
      refs.topicDetailsPanel &&
      refs.brokersPanel &&
      refs.groupsPanel
    );
  }

  window.KafkaUIDom = {
    refs: refs,
    isReady: isReady,
  };
})(window);
