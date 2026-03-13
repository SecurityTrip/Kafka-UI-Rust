(function () {
  var REFRESH_MS = 5000;
  var refreshButton = document.getElementById("refresh-button");
  var liveStatus = document.getElementById("live-status");
  var bootstrapServers = document.getElementById("bootstrap-servers");
  var clusterId = document.getElementById("cluster-id");
  var controllerId = document.getElementById("controller-id");
  var brokerCount = document.getElementById("broker-count");
  var topicCount = document.getElementById("topic-count");
  var groupCount = document.getElementById("group-count");
  var brokersTitle = document.getElementById("brokers-title");
  var topicsTitle = document.getElementById("topics-title");
  var groupsTitle = document.getElementById("groups-title");
  var brokersBody = document.getElementById("brokers-body");
  var topicsBody = document.getElementById("topics-body");
  var groupsBody = document.getElementById("groups-body");

  if (
    !refreshButton ||
    !liveStatus ||
    !bootstrapServers ||
    !clusterId ||
    !controllerId ||
    !brokerCount ||
    !topicCount ||
    !groupCount ||
    !brokersTitle ||
    !topicsTitle ||
    !groupsTitle ||
    !brokersBody ||
    !topicsBody ||
    !groupsBody
  ) {
    return;
  }

  var inFlight = false;

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function setStatus(text, isError) {
    liveStatus.textContent = text;
    if (isError) {
      liveStatus.classList.add("is-error");
      return;
    }
    liveStatus.classList.remove("is-error");
  }

  function renderSnapshot(snapshot) {
    bootstrapServers.textContent = snapshot.cluster.bootstrap_servers;
    clusterId.textContent = snapshot.cluster.cluster_id;
    controllerId.textContent = String(snapshot.cluster.controller_id);
    brokerCount.textContent = String(snapshot.cluster.broker_count);
    topicCount.textContent = String(snapshot.cluster.topic_count);
    groupCount.textContent = String(snapshot.cluster.consumer_group_count);

    brokersTitle.textContent = "Brokers (" + snapshot.brokers.length + ")";
    topicsTitle.textContent = "Topics (" + snapshot.topics.length + ")";
    groupsTitle.textContent = "Consumer Groups (" + snapshot.groups.length + ")";

    brokersBody.innerHTML = snapshot.brokers
      .map(function (broker) {
        return "<tr><td>" +
          escapeHtml(broker.id) +
          "</td><td>" +
          escapeHtml(broker.host) +
          "</td><td>" +
          escapeHtml(broker.port) +
          "</td></tr>";
      })
      .join("");

    topicsBody.innerHTML = snapshot.topics
      .map(function (topic) {
        return "<tr><td>" +
          escapeHtml(topic.name) +
          "</td><td>" +
          escapeHtml(topic.partitions) +
          "</td><td>" +
          escapeHtml(topic.replication_factor) +
          "</td><td>" +
          (topic.is_internal ? "yes" : "no") +
          "</td></tr>";
      })
      .join("");

    groupsBody.innerHTML = snapshot.groups
      .map(function (group) {
        return "<tr><td>" +
          escapeHtml(group.name) +
          "</td><td>" +
          escapeHtml(group.state) +
          "</td><td>" +
          escapeHtml(group.members) +
          "</td></tr>";
      })
      .join("");
  }

  async function refreshData(manual) {
    if (inFlight) {
      return;
    }

    inFlight = true;
    if (manual) {
      setStatus("Refreshing...", false);
    }

    try {
      var response = await fetch("/api/snapshot", { cache: "no-store" });
      if (!response.ok) {
        throw new Error("request failed: " + response.status);
      }

      var snapshot = await response.json();
      renderSnapshot(snapshot);

      var updatedAt = new Date().toLocaleTimeString();
      setStatus("Auto refresh every 5s | Last update " + updatedAt, false);
    } catch (_error) {
      setStatus("Auto refresh failed. Retrying...", true);
    } finally {
      inFlight = false;
    }
  }

  refreshButton.addEventListener("click", function () {
    refreshData(true);
  });

  setInterval(function () {
    refreshData(false);
  }, REFRESH_MS);
})();
