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
    brokersSearch: document.getElementById("brokers-search"),
    brokerKpiCount: document.getElementById("broker-kpi-count"),
    brokerKpiController: document.getElementById("broker-kpi-controller"),
    brokerKpiVersion: document.getElementById("broker-kpi-version"),
    brokerKpiOnline: document.getElementById("broker-kpi-online"),
    brokerKpiOnlineTotal: document.getElementById("broker-kpi-online-total"),
    brokerKpiUrp: document.getElementById("broker-kpi-urp"),
    brokerKpiIsr: document.getElementById("broker-kpi-isr"),
    brokerKpiIsrTotal: document.getElementById("broker-kpi-isr-total"),
    brokerKpiOosr: document.getElementById("broker-kpi-oosr"),
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
    brokerDetailsEmpty: document.getElementById("broker-details-empty"),
    brokerDetailsGrid: document.getElementById("broker-details-grid"),
    detailBrokerName: document.getElementById("detail-broker-name"),
    detailBrokerRole: document.getElementById("detail-broker-role"),
    detailBrokerEndpoint: document.getElementById("detail-broker-endpoint"),
    detailBrokerLeaders: document.getElementById("detail-broker-leaders"),
    detailBrokerReplicas: document.getElementById("detail-broker-replicas"),
    detailBrokerIsr: document.getElementById("detail-broker-isr"),
    navItems: document.querySelectorAll(".nav-item[data-view]"),
    overviewPanel: document.getElementById("overview-panel"),
    topicsPanel: document.getElementById("topics-panel"),
    topicDetailsPanel: document.getElementById("topic-details-panel"),
    brokersPanel: document.getElementById("brokers-panel"),
    brokerDetailsPanel: document.getElementById("broker-details-panel"),
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
      refs.brokersSearch &&
      refs.brokerKpiCount &&
      refs.brokerKpiController &&
      refs.brokerKpiVersion &&
      refs.brokerKpiOnline &&
      refs.brokerKpiOnlineTotal &&
      refs.brokerKpiUrp &&
      refs.brokerKpiIsr &&
      refs.brokerKpiIsrTotal &&
      refs.brokerKpiOosr &&
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
      refs.brokerDetailsEmpty &&
      refs.brokerDetailsGrid &&
      refs.detailBrokerName &&
      refs.detailBrokerRole &&
      refs.detailBrokerEndpoint &&
      refs.detailBrokerLeaders &&
      refs.detailBrokerReplicas &&
      refs.detailBrokerIsr &&
      refs.overviewPanel &&
      refs.topicsPanel &&
      refs.topicDetailsPanel &&
      refs.brokersPanel &&
      refs.brokerDetailsPanel &&
      refs.groupsPanel
    );
  }

  window.KafkaUIDom = {
    refs: refs,
    isReady: isReady,
  };
})(window);
