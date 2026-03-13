(function (window) {
  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function setStatus(text, isError) {
    var refs = window.KafkaUIDom.refs;
    refs.liveStatus.textContent = text;
    if (isError) {
      refs.liveStatus.classList.add("is-error");
      return;
    }
    refs.liveStatus.classList.remove("is-error");
  }

  function stateClass(state) {
    var normalized = String(state || "").toLowerCase();
    if (normalized.indexOf("stable") >= 0) {
      return "is-stable";
    }
    if (normalized.indexOf("empty") >= 0) {
      return "is-empty";
    }
    if (normalized.indexOf("dead") >= 0 || normalized.indexOf("unknown") >= 0) {
      return "is-dead";
    }
    return "is-unknown";
  }

  function topicMatchesFilter(topic) {
    var refs = window.KafkaUIDom.refs;
    var q = refs.topicsSearch.value.trim().toLowerCase();
    if (!q) {
      return true;
    }

    return (
      String(topic.name).toLowerCase().indexOf(q) >= 0 ||
      String(topic.partitions).indexOf(q) >= 0 ||
      String(topic.replication_factor).indexOf(q) >= 0
    );
  }

  function groupMatchesFilter(group) {
    var refs = window.KafkaUIDom.refs;
    var q = refs.groupsSearch.value.trim().toLowerCase();
    if (!q) {
      return true;
    }

    return (
      String(group.name).toLowerCase().indexOf(q) >= 0 ||
      String(group.state).toLowerCase().indexOf(q) >= 0 ||
      String(group.members).indexOf(q) >= 0
    );
  }

  function brokerMatchesFilter(broker) {
    var refs = window.KafkaUIDom.refs;
    var q = refs.brokersSearch.value.trim().toLowerCase();
    if (!q) {
      return true;
    }

    var endpoint = String(broker.host) + ":" + String(broker.port);
    return (
      String(broker.id).indexOf(q) >= 0 ||
      String(broker.host).toLowerCase().indexOf(q) >= 0 ||
      String(broker.port).indexOf(q) >= 0 ||
      endpoint.toLowerCase().indexOf(q) >= 0
    );
  }

  function brokerStatsById(snapshot) {
    var stats = {};
    (snapshot.brokers || []).forEach(function (broker) {
      var id = String(broker.id);
      stats[id] = { leaders: 0, replicas: 0, isr: 0 };
    });

    (snapshot.topics || []).forEach(function (topic) {
      (topic.partition_details || []).forEach(function (partition) {
        var leaderKey = String(partition.leader);
        if (!stats[leaderKey]) {
          stats[leaderKey] = { leaders: 0, replicas: 0, isr: 0 };
        }
        stats[leaderKey].leaders += 1;

        (partition.replicas || []).forEach(function (replicaId) {
          var replicaKey = String(replicaId);
          if (!stats[replicaKey]) {
            stats[replicaKey] = { leaders: 0, replicas: 0, isr: 0 };
          }
          stats[replicaKey].replicas += 1;
        });

        (partition.isr || []).forEach(function (isrId) {
          var isrKey = String(isrId);
          if (!stats[isrKey]) {
            stats[isrKey] = { leaders: 0, replicas: 0, isr: 0 };
          }
          stats[isrKey].isr += 1;
        });
      });
    });

    return stats;
  }

  function clusterPartitionKpis(snapshot) {
    var totals = {
      totalPartitions: 0,
      onlinePartitions: 0,
      totalReplicaAssignments: 0,
      totalIsrAssignments: 0,
      urpAssignments: 0,
    };

    (snapshot.topics || []).forEach(function (topic) {
      (topic.partition_details || []).forEach(function (partition) {
        totals.totalPartitions += 1;
        if (Number(partition.leader) >= 0) {
          totals.onlinePartitions += 1;
        }

        var replicaCount = (partition.replicas || []).length;
        var isrCount = (partition.isr || []).length;

        totals.totalReplicaAssignments += replicaCount;
        totals.totalIsrAssignments += isrCount;
        if (replicaCount > isrCount) {
          totals.urpAssignments += replicaCount - isrCount;
        }
      });
    });

    return totals;
  }

  function renderTopicDetails(topic) {
    var refs = window.KafkaUIDom.refs;

    if (!topic) {
      refs.detailsEmpty.hidden = false;
      refs.detailsGrid.hidden = true;
      refs.partitionsWrap.hidden = true;
      refs.topicPartitionsBody.innerHTML = "";
      return;
    }

    refs.detailsEmpty.hidden = true;
    refs.detailsGrid.hidden = false;
    refs.partitionsWrap.hidden = false;

    refs.detailTopicName.textContent = topic.name;
    refs.detailTopicPartitions.textContent = String(topic.partitions);
    refs.detailTopicReplication.textContent = String(topic.replication_factor);
    refs.detailTopicInternal.textContent = topic.is_internal ? "yes" : "no";

    refs.topicPartitionsBody.innerHTML = (topic.partition_details || [])
      .map(function (partition) {
        return (
          "<tr><td>" +
          escapeHtml(partition.id) +
          "</td><td>" +
          escapeHtml(partition.leader) +
          "</td><td>" +
          escapeHtml((partition.replicas || []).join(", ")) +
          "</td><td>" +
          escapeHtml((partition.isr || []).join(", ")) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function syncSelectedTopic() {
    var state = window.KafkaUIState;
    var snapshot = state.latestSnapshot;

    if (!snapshot) {
      renderTopicDetails(null);
      return;
    }

    var topics = snapshot.topics || [];
    if (!topics.length) {
      state.selectedTopic = null;
      renderTopicDetails(null);
      return;
    }

    if (!state.selectedTopic || !topics.some(function (t) { return t.name === state.selectedTopic; })) {
      state.selectedTopic = topics[0].name;
    }

    var activeTopic = null;
    for (var i = 0; i < topics.length; i += 1) {
      if (topics[i].name === state.selectedTopic) {
        activeTopic = topics[i];
        break;
      }
    }

    renderTopicDetails(activeTopic);
  }

  function renderBrokerDetails(broker, stats, controllerId) {
    var refs = window.KafkaUIDom.refs;
    if (!broker) {
      refs.brokerDetailsEmpty.hidden = false;
      refs.brokerDetailsGrid.hidden = true;
      return;
    }

    var role = String(broker.id) === String(controllerId) ? "Controller" : "Follower";
    var brokerStats = stats[String(broker.id)] || { leaders: 0, replicas: 0, isr: 0 };

    refs.brokerDetailsEmpty.hidden = true;
    refs.brokerDetailsGrid.hidden = false;
    refs.detailBrokerName.textContent = "broker-" + String(broker.id);
    refs.detailBrokerRole.textContent = role;
    refs.detailBrokerEndpoint.textContent = String(broker.host) + ":" + String(broker.port);
    refs.detailBrokerLeaders.textContent = String(brokerStats.leaders);
    refs.detailBrokerReplicas.textContent = String(brokerStats.replicas);
    refs.detailBrokerIsr.textContent = String(brokerStats.isr);
  }

  function syncSelectedBroker() {
    var state = window.KafkaUIState;
    var snapshot = state.latestSnapshot;

    if (!snapshot) {
      renderBrokerDetails(null, {}, 0);
      return;
    }

    var brokers = snapshot.brokers || [];
    if (!brokers.length) {
      state.selectedBroker = null;
      renderBrokerDetails(null, {}, snapshot.cluster.controller_id);
      return;
    }

    if (
      state.selectedBroker === null ||
      !brokers.some(function (b) {
        return String(b.id) === String(state.selectedBroker);
      })
    ) {
      state.selectedBroker = brokers[0].id;
    }

    var selected = null;
    for (var i = 0; i < brokers.length; i += 1) {
      if (String(brokers[i].id) === String(state.selectedBroker)) {
        selected = brokers[i];
        break;
      }
    }

    renderBrokerDetails(selected, brokerStatsById(snapshot), snapshot.cluster.controller_id);
  }

  function bindTopicSelection() {
    var refs = window.KafkaUIDom.refs;
    refs.topicsBody.addEventListener("click", function (event) {
      var button = event.target.closest(".topic-link");
      if (!button) {
        return;
      }

      window.KafkaUIState.selectedTopic = button.getAttribute("data-topic-name");
      rerender();
    });
  }

  function bindBrokerSelection() {
    var refs = window.KafkaUIDom.refs;
    refs.brokersBody.addEventListener("click", function (event) {
      var button = event.target.closest(".broker-link");
      if (!button) {
        return;
      }

      window.KafkaUIState.selectedBroker = button.getAttribute("data-broker-id");
      rerender();
    });
  }

  function renderSnapshot(snapshot) {
    if (!snapshot) {
      return;
    }

    var refs = window.KafkaUIDom.refs;
    var state = window.KafkaUIState;

    state.latestSnapshot = snapshot;

    refs.bootstrapServers.textContent = snapshot.cluster.bootstrap_servers;
    refs.clusterId.textContent = snapshot.cluster.cluster_id;
    refs.controllerId.textContent = String(snapshot.cluster.controller_id);
    refs.brokerCount.textContent = String(snapshot.cluster.broker_count);

    var partitionKpis = clusterPartitionKpis(snapshot);
    refs.brokerKpiCount.textContent = String(snapshot.cluster.broker_count);
    refs.brokerKpiController.textContent = String(snapshot.cluster.controller_id);
    refs.brokerKpiVersion.textContent = String(snapshot.cluster.kafka_version || "Unknown");
    refs.brokerKpiOnline.textContent = String(partitionKpis.onlinePartitions);
    refs.brokerKpiOnlineTotal.textContent = String(partitionKpis.totalPartitions);
    refs.brokerKpiUrp.textContent = String(partitionKpis.urpAssignments);
    refs.brokerKpiIsr.textContent = String(partitionKpis.totalIsrAssignments);
    refs.brokerKpiIsrTotal.textContent = String(partitionKpis.totalReplicaAssignments);
    refs.brokerKpiOosr.textContent = String(
      Math.max(0, partitionKpis.totalReplicaAssignments - partitionKpis.totalIsrAssignments)
    );

    var visibleTopics = (snapshot.topics || []).filter(topicMatchesFilter);
    var visibleBrokers = (snapshot.brokers || []).filter(brokerMatchesFilter);
    var visibleGroups = (snapshot.groups || []).filter(groupMatchesFilter);

    refs.topicCount.textContent = String(visibleTopics.length);
    refs.groupCount.textContent = String(visibleGroups.length);
    refs.brokersTitle.textContent = "Brokers (" + visibleBrokers.length + ")";
    refs.topicsTitle.textContent = "Topics (" + visibleTopics.length + ")";
    refs.groupsTitle.textContent = "Consumer Groups (" + visibleGroups.length + ")";

    refs.brokersBody.innerHTML = visibleBrokers
      .map(function (broker) {
        var isController = String(broker.id) === String(snapshot.cluster.controller_id);
        var selectedClass = String(window.KafkaUIState.selectedBroker) === String(broker.id) ? " is-selected" : "";
        var role = isController
          ? "<span class=\"role-pill\">Controller<span class=\"controller-check\" aria-label=\"active controller\">&#10003;</span></span>"
          : "<span class=\"role-pill is-follower\">Follower</span>";
        return (
          "<tr class=\"broker-row" +
          selectedClass +
          "\"><td><button class=\"broker-link\" type=\"button\" data-broker-id=\"" +
          escapeHtml(broker.id) +
          "\">broker-" +
          escapeHtml(broker.id) +
          (isController
            ? "<span class=\"controller-check\" aria-label=\"active controller\">&#10003;</span>"
            : "") +
          "</button></td><td>" +
          escapeHtml(broker.host) +
          "</td><td>" +
          escapeHtml(broker.port) +
          "</td><td>" +
          role +
          "</td><td>" +
          escapeHtml(String(broker.host) + ":" + String(broker.port)) +
          "</td></tr>"
        );
      })
      .join("");

    refs.topicsBody.innerHTML = visibleTopics
      .map(function (topic) {
        var selectedClass = state.selectedTopic === topic.name ? " is-selected" : "";
        return (
          "<tr class=\"topic-row" +
          selectedClass +
          "\"><td><button class=\"topic-link\" type=\"button\" data-topic-name=\"" +
          escapeHtml(topic.name) +
          "\">" +
          escapeHtml(topic.name) +
          "</button></td><td>" +
          escapeHtml(topic.partitions) +
          "</td><td>" +
          escapeHtml(topic.replication_factor) +
          "</td><td>" +
          (topic.is_internal ? "yes" : "no") +
          "</td></tr>"
        );
      })
      .join("");

    refs.groupsBody.innerHTML = visibleGroups
      .map(function (group) {
        var klass = stateClass(group.state);
        return (
          "<tr><td>" +
          escapeHtml(group.name) +
          "</td><td><span class=\"state-pill " +
          klass +
          "\">" +
          escapeHtml(group.state) +
          "</span></td><td>" +
          escapeHtml(group.members) +
          "</td></tr>"
        );
      })
      .join("");

    syncSelectedTopic();
    syncSelectedBroker();
  }

  function rerender() {
    renderSnapshot(window.KafkaUIState.latestSnapshot);
  }

  function bindFilters() {
    var refs = window.KafkaUIDom.refs;
    refs.brokersSearch.addEventListener("input", rerender);
    refs.topicsSearch.addEventListener("input", rerender);
    refs.groupsSearch.addEventListener("input", rerender);
  }

  function init() {
    bindTopicSelection();
    bindBrokerSelection();
    bindFilters();
  }

  window.KafkaUIRender = {
    init: init,
    setStatus: setStatus,
    renderSnapshot: renderSnapshot,
    rerender: rerender,
  };
})(window);
