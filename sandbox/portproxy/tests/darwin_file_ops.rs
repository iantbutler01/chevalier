#![cfg(target_os = "macos")]

pub mod pb {
    pub mod bracket {
        pub mod portproxy {
            pub mod v1 {
                tonic::include_proto!("bracket.portproxy.v1");
            }
        }
    }

    pub mod google {
        pub mod protobuf {
            tonic::include_proto!("google.protobuf");
        }
    }
}

use std::net::TcpListener;
use std::process::Stdio;
use std::time::Duration;

use pb::bracket::portproxy::v1::port_proxy_client::PortProxyClient;
use pb::bracket::portproxy::v1::{
    DeletePathRequest, ListDirectoryRequest, ReadFileRequest, WriteFileRequest,
};
use tokio::process::{Child, Command};
use tonic::transport::Channel;
use tonic::{Code, Request};

const TOKEN: &str = "darwin-file-ops-test";

struct GuestAgent {
    child: Child,
}

impl Drop for GuestAgent {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    request
}

async fn start_guest_agent() -> (GuestAgent, PortProxyClient<Channel>) {
    let rpc_port = TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(env!("CARGO_BIN_EXE_portproxy"))
        .args([
            "--server",
            "--rpc-port",
            &rpc_port.to_string(),
            "--server-addr",
            "127.0.0.1:0",
        ])
        .env("CHEVALIER_PORTPROXY_AUTH_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let endpoint = format!("http://127.0.0.1:{rpc_port}");
    for _ in 0..100 {
        if let Ok(client) = PortProxyClient::connect(endpoint.clone()).await {
            return (GuestAgent { child }, client);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("Darwin portproxy did not become ready at {endpoint}");
}

#[tokio::test]
async fn authenticated_file_operations_reach_the_real_darwin_guest_binary() {
    let (_agent, mut client) = start_guest_agent().await;
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("nested");
    let file = parent.join("roundtrip.txt");
    let payload = b"darwin portproxy file operation roundtrip".to_vec();

    client
        .write_file(authorized(WriteFileRequest {
            path: file.to_string_lossy().into_owned(),
            data: payload.clone(),
            create_parents: true,
        }))
        .await
        .unwrap();

    let read = client
        .read_file(authorized(ReadFileRequest {
            path: file.to_string_lossy().into_owned(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.data, payload);

    let listing = client
        .list_directory(authorized(ListDirectoryRequest {
            path: parent.to_string_lossy().into_owned(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listing.entries.len(), 1);
    assert_eq!(listing.entries[0].name, "roundtrip.txt");
    assert!(!listing.entries[0].is_dir);
    assert!(!listing.entries[0].is_symlink);

    client
        .delete_path(authorized(DeletePathRequest {
            path: parent.to_string_lossy().into_owned(),
        }))
        .await
        .unwrap();

    let error = client
        .read_file(authorized(ReadFileRequest {
            path: file.to_string_lossy().into_owned(),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
}
