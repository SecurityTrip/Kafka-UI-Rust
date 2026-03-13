(function (window) {
  function sectionsForView(view) {
    if (view === "brokers") {
      return { topics: false, details: false, brokers: true, brokerDetails: true, groups: false };
    }
    if (view === "groups") {
      return { topics: false, details: false, brokers: false, brokerDetails: false, groups: true };
    }
    if (view === "overview") {
      return { topics: true, details: false, brokers: true, brokerDetails: false, groups: true };
    }
    return { topics: true, details: true, brokers: false, brokerDetails: false, groups: false };
  }

  function applyView(view) {
    var refs = window.KafkaUIDom.refs;
    var state = window.KafkaUIState;
    var layout = sectionsForView(view);

    state.activeView = view;

    refs.topicsPanel.classList.toggle("is-hidden", !layout.topics);
    refs.topicDetailsPanel.classList.toggle("is-hidden", !layout.details);
    refs.brokersPanel.classList.toggle("is-hidden", !layout.brokers);
    refs.brokerDetailsPanel.classList.toggle("is-hidden", !layout.brokerDetails);
    refs.groupsPanel.classList.toggle("is-hidden", !layout.groups);

    refs.navItems.forEach(function (item) {
      var isActive = item.getAttribute("data-view") === view;
      item.classList.toggle("is-active", isActive);
    });
  }

  function resolveViewFromHash() {
    var hash = (window.location.hash || "").replace("#", "");
    if (hash.indexOf("topics/") === 0) {
      return "topics";
    }
    if (hash === "brokers" || hash === "groups" || hash === "overview" || hash === "topics") {
      return hash;
    }
    return "topics";
  }

  function bindNav() {
    var refs = window.KafkaUIDom.refs;

    refs.navItems.forEach(function (item) {
      item.addEventListener("click", function () {
        var targetView = item.getAttribute("data-view") || "topics";
        window.location.hash = targetView;
      });
    });

    window.addEventListener("hashchange", function () {
      applyView(resolveViewFromHash());
    });
  }

  function init() {
    bindNav();
    applyView(resolveViewFromHash());
  }

  window.KafkaUINavigation = {
    init: init,
    applyView: applyView,
  };
})(window);
