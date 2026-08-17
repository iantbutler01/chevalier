// @dive-file: CLI argument contract for server/client portproxy modes.
// @dive-rel: Consumed by portproxy/src/main.rs startup path to select proxy behavior.
// @dive-rel: Validation enforces required client forwarding arguments before runtime boot.

use std::net::IpAddr;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about = "Port proxy server and client")]
pub struct Args {
    /// Run in server mode
    #[arg(long)]
    pub server: bool,

    /// Port for the gRPC server (server mode)
    #[arg(long = "rpc-port", default_value_t = 13_338)]
    pub rpc_port: u16,

    /// Address on which the gRPC server listens (server mode)
    #[arg(long = "rpc-bind-address", default_value = "0.0.0.0")]
    pub rpc_bind_address: IpAddr,

    /// Address for the TCP proxy listener (server mode) or remote server (client mode)
    #[arg(long = "server-addr", default_value = "0.0.0.0:13337")]
    pub server_addr: String,

    /// Port to listen on (client mode)
    #[arg(long = "listen-port")]
    pub listen_port: Option<u16>,

    /// Destination port to forward to (client mode)
    #[arg(long = "forward-port")]
    pub forward_port: Option<u16>,
}

impl Args {
    pub fn validate(&self) -> Result<(), String> {
        if self.server {
            return Ok(());
        }
        match (self.listen_port, self.forward_port) {
            (Some(_), Some(_)) => Ok(()),
            _ => Err("Client mode requires --listen-port and --forward-port".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[test]
    fn parses_loopback_rpc_bind_address() {
        let args =
            Args::try_parse_from(["portproxy", "--server", "--rpc-bind-address", "127.0.0.1"])
                .unwrap();

        assert_eq!(args.rpc_bind_address, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(args.rpc_port, 13_338);
    }

    #[test]
    fn preserves_wildcard_rpc_bind_default() {
        let args = Args::try_parse_from(["portproxy", "--server"]).unwrap();

        assert_eq!(args.rpc_bind_address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }
}
