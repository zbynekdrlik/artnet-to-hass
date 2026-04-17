use artnet_to_hass::{artnet, config};

fn main() {
    let _ = (artnet::parse_artdmx, config::Config::from_env);
    println!("artnet-to-hass: scaffold only");
}
