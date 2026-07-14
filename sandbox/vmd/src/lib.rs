// @dive-file: Root module export surface for vmd runtime subsystems and generated protobuf bindings.
// @dive-rel: Exposes control bus, app wiring, and VM manager modules consumed by vmd binaries/tests.
// @dive-rel: Includes both vmd and portproxy protobuf packages so node-local control workers can invoke guest exec RPCs.
#![allow(clippy::clone_on_copy)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::manual_div_ceil)]
#![allow(clippy::manual_find)]
#![allow(clippy::nonminimal_bool)]
#![allow(clippy::ptr_arg)]
#![allow(clippy::unnecessary_cast)]

pub mod app;
pub mod assets;
pub mod bootstrap;
pub mod config;
pub mod control_bus;
pub mod fuse;
pub mod guest_exec_probe;
pub mod health_reconciler;
pub mod image;
pub mod network;
pub mod partition;
pub mod pci;
pub mod public_ingress;
pub mod reconcile;
pub mod registry;
pub mod state;
pub mod virt;

pub mod proto {
    pub mod bracket {
        pub mod portproxy {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/bracket.portproxy.v1.rs"));
            }
        }
    }

    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/vmd.v1.rs"));
    }
}
