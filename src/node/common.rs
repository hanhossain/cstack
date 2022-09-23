use crate::node::NodeType;
use crate::pager::PAGE_SIZE;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

// Common Node Header Layout
pub const HEADER_SIZE: usize = size_of::<Header>();

#[derive(Serialize, Deserialize)]
pub struct Header {
    pub r#type: NodeType,
    pub is_root: u8,
    pub parent: u32,
}

#[derive(Debug)]
pub struct CommonNode {
    pub buffer: *mut u8,
}

impl CommonNode {
    /// Creates a CommonNode.
    pub fn new(buffer: *mut u8) -> CommonNode {
        CommonNode { buffer }
    }

    /// Gets the buffer as a slice.
    pub fn get_buffer(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer, PAGE_SIZE) }
    }

    /// Gets the buffer as a mut slice.
    pub fn get_buffer_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.buffer, PAGE_SIZE) }
    }

    /// Gets the node type.
    pub fn node_type(&self) -> NodeType {
        let buffer = self.get_buffer();
        let header: Header = bincode::deserialize(buffer).unwrap();
        header.r#type
    }

    /// Sets the node type.
    pub fn set_node_type(&mut self, node_type: NodeType) {
        let buffer = self.get_buffer_mut();
        let mut header: Header = bincode::deserialize(buffer).unwrap();
        header.r#type = node_type;
        bincode::serialize_into(buffer, &header).unwrap();
    }

    /// Gets whether this node is the root.
    pub fn is_root(&self) -> bool {
        let buffer = self.get_buffer();
        let header: Header = bincode::deserialize(buffer).unwrap();
        header.is_root != 0
    }

    /// Sets whether this node is the root.
    pub fn set_root(&mut self, is_root: bool) {
        let buffer = self.get_buffer_mut();
        let value = if is_root { 1 } else { 0 };
        let mut header: Header = bincode::deserialize(buffer).unwrap();
        header.is_root = value;
        bincode::serialize_into(buffer, &header).unwrap();
    }

    /// Gets the location for the parent node.
    pub fn parent(&self) -> u32 {
        let buffer = self.get_buffer();
        let header: Header = bincode::deserialize(buffer).unwrap();
        header.parent
    }

    /// Sets the location for the parent node.
    pub(crate) fn set_parent(&mut self, parent: u32) {
        let buffer = self.get_buffer_mut();
        let mut header: Header = bincode::deserialize(buffer).unwrap();
        header.parent = parent;
        bincode::serialize_into(buffer, &header).unwrap();
    }
}
