document.addEventListener("DOMContentLoaded", function () {
  const distanceSlider = document.getElementById("distance");
  const distanceValue = document.getElementById("distance-value");
  const fareSlider = document.getElementById("fare");
  const fareValue = document.getElementById("fare-value");

  // gradient: green → yellowgreen → orange → green for distance slider
  function updateLinearSliderColor(slider) {
    const value = Number(slider.value);
    const max = Number(slider.max);
    const percent = (value / max) * 100;

    let color;
    if (percent < 33) {
      color = `linear-gradient(to right, green 0%, yellow ${percent}%, #ddd ${percent}%, #ddd 100%)`;
    } else if (percent < 66) {
      color = `linear-gradient(to right, green 0%, yellow 33%, orange ${percent}%, #ddd ${percent}%, #ddd 100%)`;
    } else {
      color = `linear-gradient(to right, green 0%, yellow 33%, orange 66%, red ${percent}%, #ddd ${percent}%, #ddd 100%)`;
    }
    slider.style.background = color;
  }


  // gradient: green → yellow → orange → red for fare slider
  function updateReverseSliderColor(slider) {
    const value = Number(slider.value);
    const max = Number(slider.max);
    const percent = (value / max) * 100;

    let color;
    if (percent < 33) {
      color = `linear-gradient(to right, green 0%, yellow ${percent}%, #ddd ${percent}%, #ddd 100%)`;
    } else if (percent < 66) {
      color = `linear-gradient(to right, green 0%, yellow 33%, orange ${percent}%, #ddd ${percent}%, #ddd 100%)`;
    } else {
      color = `linear-gradient(to right, green 0%, yellow 33%, orange 66%, red ${percent}%, #ddd ${percent}%, #ddd 100%)`;
    }
    slider.style.background = color;
  }

  distanceSlider.oninput = function () {
    distanceValue.textContent = this.value;
    updateLinearSliderColor(this);
  };

  fareSlider.oninput = function () {
    fareValue.textContent = this.value;
    updateReverseSliderColor(this);
  };

  // initial update on page load
  updateLinearSliderColor(distanceSlider);
  updateReverseSliderColor(fareSlider);

  document.getElementById("apply-filters").onclick = function () {
    fetchAndUpdate();
  };

  function buildQueryParams() {
    const date = document.getElementById("date").value;
    const hour = document.getElementById("time").value;
    const distance = distanceSlider.value;
    const zone = document.getElementById("zone").value;
    const fare = fareSlider.value;

    let params = [];
    if (date) params.push(`date=${encodeURIComponent(date)}`);
    if (hour) params.push(`hour=${hour}`);
    if (distance) params.push(`distance=${distance}`);
    if (zone) params.push(`zone=${encodeURIComponent(zone)}`);
    if (fare) params.push(`fare=${fare}`);
    return params.length ? "?" + params.join("&") : "";
  }

  function fetchAndUpdate() {
    const params = buildQueryParams();

    fetch(`/api/trips/summary${params}`)
      .then(res => res.json())
      .then(data => {
        document.getElementById("trip-count").textContent = `Trips: ${data.total_trips}`;
        document.getElementById("avg-duration").textContent = `Avg Duration: ${data.avg_duration_sec} sec`;
        document.getElementById("busiest-hour").textContent = `Busiest Hour: ${data.busiest_hour}`;
      });

    fetch(`/api/trips${params}`)
      .then(res => res.json())
      .then(renderTripTable);

    fetch(`/api/trips/time-distribution${params}`)
      .then(res => res.json())
      .then(renderTripsOverTime);

    fetch(`/api/trips/duration-histogram${params}`)
      .then(res => res.json())
      .then(renderDurationHist);

    fetch(`/api/trips/pickup-heatmap${params}`)
      .then(res => res.json())
      .then(renderPickupHeatmap);
  }

  function renderTripTable(tripList) {
    const tbody = document.querySelector("#tripTable tbody");
    tbody.innerHTML = "";
    tripList.forEach(trip => {
      let tr = document.createElement("tr");
      tr.innerHTML = `<td>${trip.pickup_location}</td>
                      <td>${trip.dropoff_location}</td>
                      <td>${trip.duration_sec}</td>
                      <td>${trip.distance_km}</td>
                      <td>${trip.fare}</td>`;
      tbody.appendChild(tr);
    });
  }

  let tripsOverTimeChart, durationHistChart, pickupHeatmapChart;

  function renderTripsOverTime(data) {
    if (tripsOverTimeChart) tripsOverTimeChart.destroy();
    tripsOverTimeChart = new Chart(document.getElementById("tripsOverTime").getContext("2d"), {
      type: "line",
      data: {
        labels: data.hours,
        datasets: [{ label: "Trips per Hour", data: data.counts, fill: false, borderColor: "#444" }],
      },
    });
  }

  function renderDurationHist(data) {
    if (durationHistChart) durationHistChart.destroy();
    durationHistChart = new Chart(document.getElementById("durationHist").getContext("2d"), {
      type: "bar",
      data: {
        labels: data.bins,
        datasets: [{ label: "Trip Duration (sec)", data: data.counts, backgroundColor: "#aaa" }],
      },
    });
  }

  function renderPickupHeatmap(data) {
    if (pickupHeatmapChart) pickupHeatmapChart.destroy();
    pickupHeatmapChart = new Chart(document.getElementById("pickupHeatmap").getContext("2d"), {
      type: "scatter",
      data: {
        datasets: [
          {
            label: "Pickup Locations",
            data: data.locations,
            backgroundColor: "#666",
          },
        ],
      },
      options: {
        scales: {
          x: { type: "linear", position: "bottom", title: { display: true, text: "Longitude" } },
          y: { type: "linear", title: { display: true, text: "Latitude" } },
        },
      },
    });
  }

  // sorting on table headers
  document.querySelectorAll("#tripTable th").forEach(header => {
    header.onclick = function () {
      const sortBy = this.getAttribute("data-sort");
      fetch(`/api/trips?sort=${sortBy}`)
        .then(res => res.json())
        .then(renderTripTable);
    };
  });

  // initial load
  fetchAndUpdate();
});
