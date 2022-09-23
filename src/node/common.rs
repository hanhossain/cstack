use crate::node::NodeType;
use crate::pager::PAGE_SIZE;
use serde::de::Unexpected;
use serde::{Deserialize, Deserializer, Serialize};
use std::mem::size_of;

// Common Node Header Layout
pub const HEADER_SIZE: usize = size_of::<Header>();

#[derive(Debug, Serialize, Deserialize)]
pub struct Header {
    pub r#type: NodeType,
    #[serde(deserialize_with = "bool_from_int")]
    pub is_root: bool,
    pub parent: u32,
}

fn bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(serde::de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"zero or one",
        )),
    }
}

#[derive(Debug)]
pub struct CommonNode {
    pub buffer: *mut u8,
    header: Header,
}

impl CommonNode {
    /// Creates a CommonNode.
    pub fn new(buffer: *mut u8) -> CommonNode {
        let slice = unsafe { std::slice::from_raw_parts(buffer, PAGE_SIZE) };
        let header = bincode::deserialize(slice).unwrap();
        CommonNode { buffer, header }
    }

    /// Gets the buffer as a slice.
    pub fn get_buffer(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer, PAGE_SIZE) }
    }

    /// Gets the buffer as a mut slice.
    pub fn get_buffer_mut(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.buffer, PAGE_SIZE) }
    }

    /// Gets the node type.
    pub fn node_type(&self) -> NodeType {
        self.header.r#type
    }

    /// Sets the node type.
    pub fn set_node_type(&mut self, node_type: NodeType) {
        self.header.r#type = node_type;
        let buffer = self.get_buffer_mut();
        bincode::serialize_into(buffer, &self.header).unwrap();
    }

    /// Gets whether this node is the root.
    pub fn is_root(&self) -> bool {
        self.header.is_root
    }

    /// Sets whether this node is the root.
    pub fn set_root(&mut self, is_root: bool) {
        let buffer = self.get_buffer_mut();

        // TODO: Tests fail if it's not deserialized here. Not sure why yet.
        let mut header: Header = bincode::deserialize(buffer).unwrap();
        header.is_root = is_root;
        bincode::serialize_into(buffer, &header).unwrap();
        self.header.is_root = is_root;
    }

    /// Gets the location for the parent node.
    pub fn parent(&self) -> u32 {
        self.header.parent
    }

    /// Sets the location for the parent node.
    pub(crate) fn set_parent(&mut self, parent: u32) {
        let buffer = self.get_buffer_mut();

        // TODO: Tests fail if it's not deserialized here. Not sure why yet.
        let mut header: Header = bincode::deserialize(buffer).unwrap();
        header.parent = parent;
        bincode::serialize_into(buffer, &header).unwrap();
        self.header.parent = parent
    }
}
