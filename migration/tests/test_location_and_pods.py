"""Location column choice, Monday pin forwarding, state-map pods, frozen existing drivers."""
import json
from unittest.mock import patch

from migration import contractor_sync as cs

COLS = [{"id": "addr", "title": "*Address", "type": "text"},
        {"id": "loc", "title": "*Location", "type": "location"},
        {"id": "em", "title": "*Email", "type": "email"},
        {"id": "pod", "title": "Pod Color", "type": "status"}]


def test_location_maps_to_location_type_column_every_time():
    for _ in range(25):
        assert cs._discover_mapping(COLS)["location"] == "loc"


def _item(loc_text, pod="Red"):
    return {"id": "9", "name": "Tim McEwen", "created_at": "2026-01-01T00:00:00Z", "column_values": [
        {"id": "em", "text": "tim@x.com", "value": None},
        {"id": "addr", "text": "1 Main St", "value": None},
        {"id": "pod", "text": pod, "value": None},
        {"id": "loc", "text": loc_text, "value": json.dumps({"lat": "34.0", "lng": "-81.0"})},
    ]}


def test_item_to_source_full_address_pin_and_state_pod():
    src = cs._item_to_source(_item("1 Main St, Columbia, SC, USA"), cs._discover_mapping(COLS))
    assert src["location"] == "1 Main St, Columbia, SC, USA"
    assert (src["monday_lat"], src["monday_lng"]) == (34.0, -81.0)
    assert src["pod_color"] == "Blue" and cs._resolved_pod(src) == "Blue"


def test_court_is_not_connecticut():
    assert cs._infer_pod_from_location("55 Oak Ct, Phoenix, AZ") == "Orange"


def test_forward_includes_pin(monkeypatch):
    monkeypatch.setenv("REVAMP_CONTRACTOR_SYNC_URL", "https://revamp.example/internal/contractors/sync")
    monkeypatch.setenv("REVAMP_CONTRACTOR_SYNC_TOKEN", "t")
    src = cs._item_to_source(_item("1 Main St, Columbia, SC, USA"), cs._discover_mapping(COLS))
    sent = {}
    class R:
        def raise_for_status(self): pass
        def json(self): return {"added": 1, "updated": 0}
    def fake_post(url, headers, json, timeout):
        sent.update(json["contractors"][0]); return R()
    with patch.object(cs.requests, "post", fake_post):
        cs._forward_revamp_intake([src])
    assert (sent["monday_lat"], sent["monday_lng"], sent["pod_color"]) == (34.0, -81.0, "Blue")


def test_existing_onfleet_driver_is_not_touched(monkeypatch):
    teams = [{"id": "t_red", "name": "POD: Red"}, {"id": "t_blue", "name": "POD: Blue"}]
    workers = [{"id": "w1", "phone": "+18035550100", "email": "tim@x.com", "teams": ["t_red"],
                "metadata": [{"name": "Address", "value": "old address"}]}]
    calls = []
    monkeypatch.setattr(cs, "_onfleet_request", lambda *a, **k: calls.append(a))
    src = {"email": "tim@x.com", "phone": "8035550100", "name": "Tim McEwen",
           "location": "1 Main St, Columbia, SC, USA", "pod_color": "Blue"}
    assert cs._onfleet_sync_new_contractor(src, teams, workers)["status"] == "already_present"
    assert calls == []


def test_new_onfleet_driver_created_on_state_pod_team(monkeypatch):
    teams = [{"id": "t_red", "name": "POD: Red"}, {"id": "t_blue", "name": "POD: Blue"}]
    posted = {}
    class R:
        def json(self): return {"id": "new"}
    def fake_req(method, path, **kw):
        if method == "POST": posted.update(kw.get("json") or {})
        return R()
    monkeypatch.setattr(cs, "_onfleet_request", fake_req)
    monkeypatch.setattr(cs, "_onfleet_routing_destination_id", lambda a: "dest1")
    src = {"email": "new@x.com", "phone": "8035550101", "name": "New IC",
           "location": "2 Main St, Columbia, SC, USA", "pod_color": "Red"}
    assert cs._onfleet_sync_new_contractor(src, teams, [])["status"] == "created"
    assert posted["teams"] == ["t_blue"]
