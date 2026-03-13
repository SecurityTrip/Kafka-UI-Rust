(function (window) {
  if (!window.KafkaUIDom || !window.KafkaUIDom.isReady()) {
    return;
  }

  window.KafkaUIRender.init();
  window.KafkaUINavigation.init();
  window.KafkaUIStream.connect();
})(window);
