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

    var visibleTopics = (snapshot.topics || []).filter(topicMatchesFilter);
    var visibleGroups = (snapshot.groups || []).filter(groupMatchesFilter);

    refs.topicCount.textContent = String(visibleTopics.length);
    refs.groupCount.textContent = String(visibleGroups.length);
    refs.brokersTitle.textContent = "Brokers (" + (snapshot.brokers || []).length + ")";
    refs.topicsTitle.textContent = "Topics (" + visibleTopics.length + ")";
    refs.groupsTitle.textContent = "Consumer Groups (" + visibleGroups.length + ")";

    refs.brokersBody.innerHTML = (snapshot.brokers || [])
      .map(function (broker) {
        return (
          "<tr><td>" +
          escapeHtml(broker.id) +
          "</td><td>" +
          escapeHtml(broker.host) +
          "</td><td>" +
          escapeHtml(broker.port) +
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
  }

  function rerender() {
    renderSnapshot(window.KafkaUIState.latestSnapshot);
  }

  function bindFilters() {
    var refs = window.KafkaUIDom.refs;
    refs.topicsSearch.addEventListener("input", rerender);
    refs.groupsSearch.addEventListener("input", rerender);
  }

  function init() {
    bindTopicSelection();
    bindFilters();
  }

  window.KafkaUIRender = {
    init: init,
    setStatus: setStatus,
    renderSnapshot: renderSnapshot,
    rerender: rerender,
  };
})(window);
