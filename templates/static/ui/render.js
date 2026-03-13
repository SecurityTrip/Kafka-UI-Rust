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
      String(topic.replication_factor).indexOf(q) >= 0 ||
      String(topic.out_of_sync_replicas || 0).indexOf(q) >= 0 ||
      String(topic.message_count || 0).indexOf(q) >= 0
    );
  }

  function parseTopicFromHash() {
    var hash = window.location.hash || "";
    if (hash.indexOf("#topics/") !== 0) {
      return "";
    }
    try {
      return decodeURIComponent(hash.substring("#topics/".length));
    } catch (_e) {
      return "";
    }
  }

  function isTopicDetailsRoute() {
    return (window.location.hash || "").indexOf("#topics/") === 0;
  }

  function formatSize(sizeBytes) {
    if (sizeBytes === null || sizeBytes === undefined) {
      return "-";
    }
    var value = Number(sizeBytes);
    if (!isFinite(value) || value < 0) {
      return "-";
    }
    if (value < 1024) {
      return String(Math.round(value)) + " Bytes";
    }
    if (value < 1024 * 1024) {
      return (value / 1024).toFixed(1) + " KB";
    }
    return (value / (1024 * 1024)).toFixed(1) + " MB";
  }

  function formatInteger(value) {
    var n = Number(value);
    if (!isFinite(n)) {
      return "-";
    }
    return n.toLocaleString();
  }

  function formatTimestamp(ts) {
    if (ts === null || ts === undefined) {
      return "-";
    }
    var value = Number(ts);
    if (!isFinite(value)) {
      return "-";
    }
    var date = new Date(value);
    if (isNaN(date.getTime())) {
      return "-";
    }
    return date.toLocaleString();
  }

  function messageId(message) {
    return String(message.partition) + ":" + String(message.offset);
  }

  function textPreview(value) {
    if (value === null || value === undefined) {
      return "(null)";
    }
    var text = String(value);
    if (text.length <= 56) {
      return text;
    }
    return text.slice(0, 56) + "...";
  }

  function parseMaybeJson(value) {
    if (value === null || value === undefined) {
      return null;
    }

    var text = String(value);
    try {
      return JSON.parse(text);
    } catch (_e) {
      return text;
    }
  }

  function normalizePayloadTab(tab) {
    if (tab === "key" || tab === "headers" || tab === "value") {
      return tab;
    }
    return "value";
  }

  function formatMessagePayloadByTab(message, tab) {
    if (!message) {
      return "null";
    }

    var normalizedTab = normalizePayloadTab(tab);

    if (normalizedTab === "key") {
      return JSON.stringify(parseMaybeJson(message.key), null, 2);
    }

    if (normalizedTab === "headers") {
      var headers = (message.headers || []).map(function (header) {
        return {
          key: header.key,
          value: parseMaybeJson(header.value),
        };
      });
      return JSON.stringify(headers, null, 2);
    }

    return JSON.stringify(parseMaybeJson(message.value), null, 2);
  }

  function renderPayloadTabs() {
    var refs = window.KafkaUIDom.refs;
    var state = window.KafkaUIState;
    var activeTab = normalizePayloadTab(state.selectedPayloadTab);
    refs.payloadTabButtons.forEach(function (button) {
      var tab = button.getAttribute("data-payload-tab");
      button.classList.toggle("is-active", tab === activeTab);
    });
  }

  function highlightJson(jsonText) {
    var escaped = String(jsonText)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");

    return escaped.replace(
      /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"\s*:?)|(\btrue\b|\bfalse\b|\bnull\b)|(-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
      function (match) {
        var klass = "json-number";
        if (match.indexOf('"') === 0) {
          klass = /:$/.test(match) ? "json-key" : "json-string";
        } else if (/true|false/.test(match)) {
          klass = "json-boolean";
        } else if (/null/.test(match)) {
          klass = "json-null";
        }
        return '<span class="' + klass + '">' + match + "</span>";
      }
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
    var state = window.KafkaUIState;

    if (!topic) {
      refs.detailsEmpty.hidden = false;
      refs.detailsGrid.hidden = true;
      refs.partitionsWrap.hidden = true;
      refs.topicPartitionsBody.innerHTML = "";
      refs.topicOverviewHead.hidden = true;
      refs.topicOverviewStatus.hidden = true;
      refs.topicOverviewGrid.hidden = true;
      refs.topicConsumersHead.hidden = true;
      refs.topicConsumersEmpty.hidden = true;
      refs.topicConsumersWrap.hidden = true;
      refs.topicConsumersBody.innerHTML = "";
      refs.topicMessagesStatus.hidden = true;
      refs.topicMessagesWrap.hidden = true;
      refs.topicMessagesBody.innerHTML = "";
      refs.topicMessageView.hidden = true;
      state.topicOverviewTopic = null;
      state.topicOverview = null;
      state.topicOverviewLoading = false;
      state.topicOverviewError = "";
      state.topicMessagesTopic = null;
      state.topicMessages = [];
      state.selectedTopicMessageId = null;
      state.selectedPayloadTab = "value";
      state.topicMessagesLoading = false;
      state.topicMessagesError = "";
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

    refs.topicOverviewHead.hidden = false;
    refs.topicConsumersHead.hidden = false;
    renderTopicOverview();
    renderTopicMessages();
  }

  function renderTopicOverview() {
    var refs = window.KafkaUIDom.refs;
    var state = window.KafkaUIState;
    var hasTopic = !!state.topicOverviewTopic;

    if (!hasTopic) {
      refs.topicOverviewStatus.hidden = true;
      refs.topicOverviewGrid.hidden = true;
      refs.topicConsumersEmpty.hidden = true;
      refs.topicConsumersWrap.hidden = true;
      refs.topicConsumersBody.innerHTML = "";
      return;
    }

    if (state.topicOverviewLoading) {
      refs.topicOverviewStatus.hidden = false;
      refs.topicOverviewStatus.innerHTML =
        "<span class=\"inline-spinner\" aria-label=\"Loading topic overview\"></span> Loading topic overview...";
      refs.topicOverviewGrid.hidden = false;
      refs.topicOverviewType.textContent = "-";
      refs.topicOverviewIsr.textContent = "-";
      refs.topicOverviewReplicas.textContent = "-";
      refs.topicOverviewUrp.textContent = "-";
      refs.topicOverviewMessages.textContent = "-";
      refs.topicOverviewCleanup.textContent = "-";
      refs.topicOverviewSegmentSize.textContent = "-";
      refs.topicOverviewSegmentCount.textContent = "-";
      refs.topicConsumersEmpty.hidden = true;
      refs.topicConsumersWrap.hidden = true;
      refs.topicConsumersBody.innerHTML = "";
      return;
    }

    if (state.topicOverviewError) {
      if (state.topicOverview) {
        refs.topicOverviewStatus.hidden = false;
        refs.topicOverviewStatus.textContent = state.topicOverviewError;
        refs.topicOverviewGrid.hidden = false;
        return;
      }
      refs.topicOverviewStatus.hidden = false;
      refs.topicOverviewStatus.textContent = state.topicOverviewError;
      refs.topicOverviewGrid.hidden = true;
      refs.topicConsumersEmpty.hidden = true;
      refs.topicConsumersWrap.hidden = true;
      refs.topicConsumersBody.innerHTML = "";
      return;
    }

    var overview = state.topicOverview;
    if (!overview) {
      refs.topicOverviewStatus.hidden = false;
      refs.topicOverviewStatus.textContent = "No overview data available.";
      refs.topicOverviewGrid.hidden = true;
      refs.topicConsumersEmpty.hidden = true;
      refs.topicConsumersWrap.hidden = true;
      refs.topicConsumersBody.innerHTML = "";
      return;
    }

    refs.topicOverviewStatus.hidden = true;
    refs.topicOverviewStatus.textContent = "";
    refs.topicOverviewGrid.hidden = false;
    refs.topicOverviewType.textContent = String(overview.topic_type || "-");
    refs.topicOverviewIsr.textContent = formatInteger(overview.in_sync_replicas);
    refs.topicOverviewReplicas.textContent = formatInteger(overview.total_replicas);
    refs.topicOverviewUrp.textContent = formatInteger(overview.urp);
    refs.topicOverviewMessages.textContent = formatInteger(overview.message_count);
    refs.topicOverviewCleanup.textContent = String(overview.cleanup_policy || "-");
    refs.topicOverviewSegmentSize.textContent = formatSize(overview.segment_size_bytes);
    refs.topicOverviewSegmentCount.textContent =
      overview.segment_count === null || overview.segment_count === undefined
        ? "-"
        : formatInteger(overview.segment_count);

    var consumers = overview.consumers || [];
    if (!consumers.length) {
      refs.topicConsumersWrap.hidden = true;
      refs.topicConsumersBody.innerHTML = "";
      refs.topicConsumersEmpty.hidden = false;
      return;
    }

    refs.topicConsumersEmpty.hidden = true;
    refs.topicConsumersWrap.hidden = false;
    refs.topicConsumersBody.innerHTML = consumers
      .map(function (item) {
        return (
          "<tr><td>" +
          escapeHtml(item.group_id) +
          "</td><td><span class=\"state-pill " +
          stateClass(item.state) +
          "\">" +
          escapeHtml(item.state) +
          "</span></td><td>" +
          escapeHtml(item.active_consumers) +
          "</td><td>" +
          escapeHtml(item.consumer_lag) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderTopicMessages() {
    var refs = window.KafkaUIDom.refs;
    var state = window.KafkaUIState;
    var hasTopic = !!state.topicMessagesTopic;

    if (!hasTopic) {
      refs.topicMessagesStatus.hidden = true;
      refs.topicMessagesWrap.hidden = true;
      refs.topicMessageView.hidden = true;
      refs.topicMessagesBody.innerHTML = "";
      return;
    }

    if (state.topicMessagesLoading) {
      if ((state.topicMessages || []).length) {
        refs.topicMessagesStatus.hidden = true;
        refs.topicMessagesWrap.hidden = false;
        refs.topicMessageView.hidden = false;
        return;
      }
      refs.topicMessagesStatus.hidden = false;
      refs.topicMessagesStatus.textContent = "Loading topic messages...";
      refs.topicMessagesWrap.hidden = true;
      refs.topicMessageView.hidden = true;
      return;
    }

    if (state.topicMessagesError) {
      if ((state.topicMessages || []).length) {
        refs.topicMessagesStatus.hidden = false;
        refs.topicMessagesStatus.textContent = state.topicMessagesError;
        refs.topicMessagesWrap.hidden = false;
        refs.topicMessageView.hidden = false;
        return;
      }
      refs.topicMessagesStatus.hidden = false;
      refs.topicMessagesStatus.textContent = state.topicMessagesError;
      refs.topicMessagesWrap.hidden = true;
      refs.topicMessageView.hidden = true;
      return;
    }

    var messages = state.topicMessages || [];
    if (!messages.length) {
      refs.topicMessagesStatus.hidden = false;
      refs.topicMessagesStatus.textContent = "No messages found for this topic.";
      refs.topicMessagesWrap.hidden = true;
      refs.topicMessageView.hidden = true;
      return;
    }

    refs.topicMessagesStatus.hidden = true;
    refs.topicMessagesWrap.hidden = false;
    refs.topicMessagesBody.innerHTML = messages
      .map(function (message) {
        var id = messageId(message);
        var selectedClass = state.selectedTopicMessageId === id ? " is-selected" : "";
        return (
          "<tr class=\"topic-message-row" +
          selectedClass +
          "\" data-message-id=\"" +
          escapeHtml(id) +
          "\"><td>" +
          escapeHtml(message.partition) +
          "</td><td>" +
          escapeHtml(message.offset) +
          "</td><td>" +
          escapeHtml(formatTimestamp(message.timestamp_ms)) +
          "</td><td>" +
          escapeHtml(message.key_size) +
          "</td><td>" +
          escapeHtml(message.value_size) +
          "</td><td>" +
          escapeHtml(textPreview(message.value)) +
          "</td></tr>"
        );
      })
      .join("");

    var selected = null;
    for (var i = 0; i < messages.length; i += 1) {
      if (messageId(messages[i]) === state.selectedTopicMessageId) {
        selected = messages[i];
        break;
      }
    }
    if (!selected) {
      selected = messages[0];
      state.selectedTopicMessageId = messageId(selected);
    }

    refs.topicMessageView.hidden = false;
    refs.topicMessageMeta.textContent =
      "Partition " +
      selected.partition +
      " | Offset " +
      selected.offset +
      " | Timestamp " +
      formatTimestamp(selected.timestamp_ms);
    renderPayloadTabs();
    refs.topicMessagePayload.innerHTML = highlightJson(
      formatMessagePayloadByTab(selected, state.selectedPayloadTab)
    );
  }

  function loadTopicMessages(topicName, force) {
    var state = window.KafkaUIState;
    if (state.topicMessagesLoading && state.topicMessagesTopic === topicName) {
      return;
    }
    if (!force && state.topicMessagesTopic === topicName && state.topicMessages.length) {
      return;
    }

    var selectedBeforeRefresh = state.selectedTopicMessageId;
    state.topicMessagesLoading = true;
    state.topicMessagesError = "";
    if (!state.topicMessages.length) {
      renderTopicMessages();
    }

    fetch("/api/topics/" + encodeURIComponent(topicName) + "/messages?limit=80")
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Unable to load topic messages");
        }
        return response.json();
      })
      .then(function (payload) {
        if (state.topicMessagesTopic !== topicName) {
          return;
        }
        state.topicMessages = payload.items || [];
        state.topicMessagesLoading = false;
        state.topicMessagesError = "";
        if (state.topicMessages.length) {
          var hasSelected = selectedBeforeRefresh && state.topicMessages.some(function (message) {
            return messageId(message) === selectedBeforeRefresh;
          });
          state.selectedTopicMessageId = hasSelected
            ? selectedBeforeRefresh
            : messageId(state.topicMessages[0]);
        } else {
          state.selectedTopicMessageId = null;
        }
        renderTopicMessages();
      })
      .catch(function () {
        if (state.topicMessagesTopic !== topicName) {
          return;
        }
        state.topicMessagesLoading = false;
        state.topicMessagesError = "Failed to load messages for selected topic.";
        renderTopicMessages();
      });
  }

  function loadTopicOverview(topicName, force) {
    var state = window.KafkaUIState;
    if (state.topicOverviewLoading && state.topicOverviewTopic === topicName) {
      return;
    }
    if (!force && state.topicOverviewTopic === topicName && state.topicOverview) {
      return;
    }

    state.topicOverviewLoading = true;
    state.topicOverviewError = "";
    if (!state.topicOverview) {
      renderTopicOverview();
    }

    fetch("/api/topics/" + encodeURIComponent(topicName) + "/overview")
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Unable to load topic overview");
        }
        return response.json();
      })
      .then(function (payload) {
        if (state.topicOverviewTopic !== topicName) {
          return;
        }
        state.topicOverview = payload;
        state.topicOverviewLoading = false;
        state.topicOverviewError = "";
        renderTopicOverview();
      })
      .catch(function () {
        if (state.topicOverviewTopic !== topicName) {
          return;
        }
        state.topicOverviewLoading = false;
        state.topicOverviewError = "Failed to load overview for selected topic.";
        renderTopicOverview();
      });
  }

  function refreshActiveTopicDetails(force) {
    var state = window.KafkaUIState;
    if (state.activeView !== "topics") {
      return;
    }
    if (!state.selectedTopic) {
      return;
    }
    if (state.topicMessagesTopic !== state.selectedTopic) {
      return;
    }

    loadTopicMessages(state.selectedTopic, force);
    loadTopicOverview(state.selectedTopic, force);
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

    if (!isTopicDetailsRoute()) {
      state.selectedTopic = null;
      renderTopicDetails(null);
      return;
    }

    var topicFromHash = parseTopicFromHash();
    if (!topicFromHash) {
      state.selectedTopic = null;
      renderTopicDetails(null);
      return;
    }

    if (topics.some(function (t) { return t.name === topicFromHash; })) {
      state.selectedTopic = topicFromHash;
    } else {
      state.selectedTopic = null;
      renderTopicDetails(null);
      return;
    }

    var activeTopic = null;
    for (var i = 0; i < topics.length; i += 1) {
      if (topics[i].name === state.selectedTopic) {
        activeTopic = topics[i];
        break;
      }
    }

    if (!activeTopic) {
      renderTopicDetails(null);
      return;
    }

    var topicChanged = state.topicMessagesTopic !== activeTopic.name;
    if (topicChanged) {
      state.topicMessagesTopic = activeTopic.name;
      state.topicOverviewTopic = activeTopic.name;
      state.topicMessages = [];
      state.topicMessagesError = "";
      state.selectedTopicMessageId = null;
      state.topicOverview = null;
      state.topicOverviewError = "";
      state.selectedPayloadTab = "value";

      loadTopicMessages(activeTopic.name, true);
      loadTopicOverview(activeTopic.name, true);
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
      window.location.hash = "topics/" + encodeURIComponent(window.KafkaUIState.selectedTopic);
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

  function bindTopicMessages() {
    var refs = window.KafkaUIDom.refs;

    refs.topicMessagesBody.addEventListener("click", function (event) {
      var row = event.target.closest(".topic-message-row");
      if (!row) {
        return;
      }
      window.KafkaUIState.selectedTopicMessageId = row.getAttribute("data-message-id");
      renderTopicMessages();
    });

    refs.topicMessagesReload.addEventListener("click", function () {
      var state = window.KafkaUIState;
      if (!state.topicMessagesTopic) {
        return;
      }
      refreshActiveTopicDetails(true);
    });
  }

  function bindTopicDetailsBack() {
    var refs = window.KafkaUIDom.refs;
    refs.topicDetailsBack.addEventListener("click", function () {
      window.location.hash = "topics";
    });
  }

  function bindPayloadTabs() {
    var refs = window.KafkaUIDom.refs;
    refs.payloadTabButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        var state = window.KafkaUIState;
        state.selectedPayloadTab = normalizePayloadTab(button.getAttribute("data-payload-tab"));
        renderTopicMessages();
      });
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
          "\"><td>" +
          escapeHtml(broker.id) +
          "</td><td><button class=\"broker-link\" type=\"button\" data-broker-id=\"" +
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
          escapeHtml(topic.out_of_sync_replicas || 0) +
          "</td><td>" +
          escapeHtml(topic.replication_factor) +
          "</td><td>" +
          escapeHtml(topic.message_count || 0) +
          "</td><td>" +
          escapeHtml(formatSize(topic.size_bytes)) +
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
    bindTopicMessages();
    bindTopicDetailsBack();
    bindPayloadTabs();
    bindFilters();

    if (window.KafkaUIState.topicDetailsRefreshTimer) {
      clearInterval(window.KafkaUIState.topicDetailsRefreshTimer);
    }
    window.KafkaUIState.topicDetailsRefreshTimer = setInterval(function () {
      refreshActiveTopicDetails(true);
    }, 10000);

    window.addEventListener("hashchange", function () {
      if ((window.location.hash || "").indexOf("#topics/") === 0) {
        rerender();
      }
    });
  }

  window.KafkaUIRender = {
    init: init,
    setStatus: setStatus,
    renderSnapshot: renderSnapshot,
    rerender: rerender,
  };
})(window);
