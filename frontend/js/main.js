document.addEventListener("DOMContentLoaded", function () {
  // --- Auth state ---
  let accessToken = null;

  const authStatus = document.getElementById("auth-status");
  const loginForm = document.getElementById("login-form");
  const logoutBtn = document.getElementById("logout-btn");

  function setAuthStatus(text) {
    if (authStatus) authStatus.textContent = text;
  }

  function loadToken() {
    try {
      const t = window.localStorage.getItem("accessToken");
      if (t) {
        accessToken = t;
        setAuthStatus("Authenticated");
      }
    } catch {}
  }

  function saveToken(t, username) {
    accessToken = t;
    try { window.localStorage.setItem("accessToken", t); } catch {}
    if (username) setAuthStatus(`Authenticated as ${username}`);
  }

  async function login(username, password) {
    // OAuth2 Password flow uses application/x-www-form-urlencoded
    const params = new URLSearchParams();
    params.append("grant_type", "password");
    params.append("username", username);
    params.append("password", password);
    params.append("scope", "");
    try {
      console.log("Login: POST /api/auth/token (form)");
      let res = await fetch("/api/auth/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: params,
      });
      if (!res.ok) {
        console.warn("Form login failed status:", res.status);
        const txt = await res.text().catch(() => "");
        console.warn("Form login response:", txt);
        // Fallback to JSON-based endpoint in case proxy/headers content-type issues
        console.log("Login: POST /api/auth/token-json (json)");
        res = await fetch("/api/auth/token-json", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ username, password }),
        });
      }
      if (!res.ok) throw new Error(`Login failed (${res.status})`);
      const data = await res.json();
      saveToken(data.access_token, username);
      return true;
    } catch (e) {
      setAuthStatus(`Login failed: ${e.message ?? e}`);
      console.error(e);
      return false;
    }
  }

  function logout() {
    accessToken = null;
    setAuthStatus("Not authenticated");
  }

  if (loginForm) {
    loginForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const username = document.getElementById("login-username").value;
      const password = document.getElementById("login-password").value;
      const ok = await login(username, password);
      if (ok) {
        // After logging in, refresh data so protected endpoints succeed
        fetchAndUpdate();
      }
    });
  }
  if (logoutBtn) {
    logoutBtn.addEventListener("click", logout);
  }
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
  loadToken();
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

  function authHeaders() {
    return accessToken ? { Authorization: `Bearer ${accessToken}` } : {};
  }

  function fetchAndUpdate() {
    const params = buildQueryParams();

    fetch(`/api/trips/summary${params}`, { headers: { ...authHeaders() } })
      .then(res => {
        if (res.status === 401) throw new Error("Unauthorized - please login");
        return res.json();
      })
      .then(data => {
        document.getElementById("trip-count").textContent = `Trips: ${data.total_trips}`;
        document.getElementById("avg-duration").textContent = `Avg Duration: ${data.avg_duration_sec} sec`;
        document.getElementById("busiest-hour").textContent = `Busiest Hour: ${data.busiest_hour}`;
      })
      .catch(err => { setAuthStatus(err.message); });

    fetch(`/api/trips${params}`, { headers: { ...authHeaders() } })
      .then(res => { if (res.status === 401) throw new Error("Unauthorized - please login"); return res.json(); })
      .then(renderTripTable);

    fetch(`/api/trips/time-distribution${params}`, { headers: { ...authHeaders() } })
      .then(res => { if (res.status === 401) throw new Error("Unauthorized - please login"); return res.json(); })
      .then(renderTripsOverTime);

    fetch(`/api/trips/duration-histogram${params}`, { headers: { ...authHeaders() } })
      .then(res => { if (res.status === 401) throw new Error("Unauthorized - please login"); return res.json(); })
      .then(renderDurationHist);

    fetch(`/api/trips/pickup-heatmap${params}`, { headers: { ...authHeaders() } })
      .then(res => { if (res.status === 401) throw new Error("Unauthorized - please login"); return res.json(); })
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
      fetch(`/api/trips?sort=${sortBy}`, { headers: { ...authHeaders() } })
        .then(res => res.json())
        .then(renderTripTable);
    };
  });

  // initial load
  fetchAndUpdate();
});
