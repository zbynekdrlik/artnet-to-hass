use serde_json::{json, Value};

/// Build the HA WebSocket `call_service` JSON for `light.turn_on` with rgb_color.
pub fn build_turn_on(id: u64, lights: &[String], rgb: (u8, u8, u8)) -> Value {
    json!({
        "id": id,
        "type": "call_service",
        "domain": "light",
        "service": "turn_on",
        "target": {"entity_id": lights},
        "service_data": {"rgb_color": [rgb.0, rgb.1, rgb.2]}
    })
}

/// Build the HA WebSocket `call_service` JSON for `light.turn_off`.
pub fn build_turn_off(id: u64, lights: &[String]) -> Value {
    json!({
        "id": id,
        "type": "call_service",
        "domain": "light",
        "service": "turn_off",
        "target": {"entity_id": lights}
    })
}

/// Build the HA WebSocket auth message.
pub fn build_auth(token: &str) -> Value {
    json!({"type": "auth", "access_token": token})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_on_matches_ha_protocol() {
        let lights = vec!["light.a".into(), "light.b".into()];
        let msg = build_turn_on(7, &lights, (255, 80, 0));
        assert_eq!(
            msg,
            json!({
                "id": 7,
                "type": "call_service",
                "domain": "light",
                "service": "turn_on",
                "target": {"entity_id": ["light.a", "light.b"]},
                "service_data": {"rgb_color": [255, 80, 0]}
            })
        );
    }

    #[test]
    fn turn_off_matches_ha_protocol() {
        let lights = vec!["light.a".into()];
        let msg = build_turn_off(9, &lights);
        assert_eq!(
            msg,
            json!({
                "id": 9,
                "type": "call_service",
                "domain": "light",
                "service": "turn_off",
                "target": {"entity_id": ["light.a"]}
            })
        );
    }

    #[test]
    fn auth_message_format() {
        let msg = build_auth("abc.def.ghi");
        assert_eq!(msg, json!({"type": "auth", "access_token": "abc.def.ghi"}));
    }
}
