(function () {
  var refreshButton = document.getElementById("refresh-button");
  if (!refreshButton) {
    return;
  }

  refreshButton.addEventListener("click", function () {
    window.location.reload();
  });
})();
