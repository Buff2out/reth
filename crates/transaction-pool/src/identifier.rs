//! Identifier types for transactions and senders.
use alloy_primitives::{map::AddressMap, Address};

/// An internal mapping of addresses.
///
/// This assigns a _unique_ [`SenderId`] for a new [`Address`].
/// It has capacity for 2^64 unique addresses.
#[derive(Debug, Default)]
pub struct SenderIdentifiers {
    /// The identifier to use next.
    id: u64,
    /// Assigned [`SenderId`] for an [`Address`].
    address_to_id: AddressMap<SenderId>,
    /// Reverse mapping of [`SenderId`] to [`Address`].
    sender_to_address: SenderSlotMap<Address>,
}

impl SenderIdentifiers {
    /// Returns the address for the given identifier.
    pub fn address(&self, id: &SenderId) -> Option<&Address> {
        self.sender_to_address.get(id)
    }

    /// Returns the [`SenderId`] that belongs to the given address, if it exists
    pub fn sender_id(&self, addr: &Address) -> Option<SenderId> {
        self.address_to_id.get(addr).copied()
    }

    /// Returns the existing [`SenderId`] or assigns a new one if it's missing
    pub fn sender_id_or_create(&mut self, addr: Address) -> SenderId {
        self.sender_id(&addr).unwrap_or_else(|| {
            let id = self.next_id();
            self.address_to_id.insert(addr, id);
            self.sender_to_address.insert(id, addr);
            id
        })
    }

    /// Returns the existing [`SenderId`] or assigns a new one if it's missing
    pub fn sender_ids_or_create(
        &mut self,
        addrs: impl IntoIterator<Item = Address>,
    ) -> Vec<SenderId> {
        addrs.into_iter().map(|addr| self.sender_id_or_create(addr)).collect()
    }

    /// Returns the current identifier and increments the counter.
    fn next_id(&mut self) -> SenderId {
        let id = self.id;
        self.id = self.id.wrapping_add(1);
        id.into()
    }
}

/// A _unique_ identifier for a sender of an address.
///
/// This is the identifier of an internal `address` mapping that is valid in the context of this
/// program.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SenderId(u64);

impl SenderId {
    /// Returns a `Bound` for [`TransactionId`] starting with nonce `0`
    pub const fn start_bound(self) -> std::ops::Bound<TransactionId> {
        std::ops::Bound::Included(TransactionId::new(self, 0))
    }

    /// Returns a `Range` for [`TransactionId`] starting with nonce `0` and ending with nonce
    /// `u64::MAX`
    pub const fn range(self) -> std::ops::RangeInclusive<TransactionId> {
        TransactionId::new(self, 0)..=TransactionId::new(self, u64::MAX)
    }

    /// Converts the sender to a [`TransactionId`] with the given nonce.
    pub const fn into_transaction_id(self, nonce: u64) -> TransactionId {
        TransactionId::new(self, nonce)
    }

    /// Returns the inner id as a `usize` index for use with [`SenderSlotMap`].
    #[inline]
    const fn index(self) -> usize {
        self.0 as usize
    }
}

impl From<u64> for SenderId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// A dense, `Vec`-backed map keyed by [`SenderId`].
///
/// Since `SenderId` values are monotonically assigned starting from 0, this avoids all hashing
/// overhead and provides O(1) indexed access with better cache locality than a `HashMap`.
#[derive(Debug, Clone)]
pub struct SenderSlotMap<T> {
    slots: Vec<Option<T>>,
    len: usize,
}

impl<T> Default for SenderSlotMap<T> {
    fn default() -> Self {
        Self { slots: Vec::new(), len: 0 }
    }
}

impl<T> SenderSlotMap<T> {
    /// Returns a reference to the value for the given sender, if present.
    #[inline]
    pub fn get(&self, id: &SenderId) -> Option<&T> {
        self.slots.get(id.index())?.as_ref()
    }

    /// Returns a mutable reference to the value for the given sender, if present.
    #[inline]
    pub fn get_mut(&mut self, id: &SenderId) -> Option<&mut T> {
        self.slots.get_mut(id.index())?.as_mut()
    }

    /// Inserts a value for the given sender, returning the previous value if any.
    pub fn insert(&mut self, id: SenderId, value: T) -> Option<T> {
        let idx = id.index();
        if idx >= self.slots.len() {
            self.slots.resize_with(idx + 1, || None);
        }
        let old = self.slots[idx].replace(value);
        if old.is_none() {
            self.len += 1;
        }
        old
    }

    /// Removes and returns the value for the given sender.
    pub fn remove(&mut self, id: &SenderId) -> Option<T> {
        let slot = self.slots.get_mut(id.index())?;
        let old = slot.take()?;
        self.len -= 1;
        Some(old)
    }

    /// Returns `true` if the map contains a value for the given sender.
    #[inline]
    pub fn contains_key(&self, id: &SenderId) -> bool {
        self.slots.get(id.index()).is_some_and(|s| s.is_some())
    }

    /// Returns the number of occupied entries.
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if there are no occupied entries.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Removes all entries.
    pub fn clear(&mut self) {
        self.slots.clear();
        self.len = 0;
    }

    /// Returns an iterator over all values.
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().filter_map(|s| s.as_ref())
    }

    /// Returns an iterator over all `(SenderId, &T)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (SenderId, &T)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|v| (SenderId(i as u64), v)))
    }

    /// Returns a mutable reference to the value for the given sender, inserting the default if
    /// absent.
    pub fn get_or_insert_default(&mut self, id: SenderId) -> &mut T
    where
        T: Default,
    {
        let idx = id.index();
        if idx >= self.slots.len() {
            self.slots.resize_with(idx + 1, || None);
        }
        if self.slots[idx].is_none() {
            self.slots[idx] = Some(T::default());
            self.len += 1;
        }
        self.slots[idx].as_mut().unwrap()
    }

    /// Extends the map with the contents of an iterator.
    pub fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (SenderId, T)>,
    {
        for (id, value) in iter {
            self.insert(id, value);
        }
    }
}

/// A unique identifier of a transaction of a Sender.
///
/// This serves as an identifier for dependencies of a transaction:
/// A transaction with a nonce higher than the current state nonce depends on `tx.nonce - 1`.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TransactionId {
    /// Sender of this transaction
    pub sender: SenderId,
    /// Nonce of this transaction
    pub nonce: u64,
}

impl TransactionId {
    /// Create a new identifier pair
    pub const fn new(sender: SenderId, nonce: u64) -> Self {
        Self { sender, nonce }
    }

    /// Returns the [`TransactionId`] this transaction depends on.
    ///
    /// This returns `transaction_nonce - 1` if `transaction_nonce` is higher than the
    /// `on_chain_nonce`
    pub fn ancestor(transaction_nonce: u64, on_chain_nonce: u64, sender: SenderId) -> Option<Self> {
        // SAFETY: transaction_nonce > on_chain_nonce ⇒ transaction_nonce >= 1
        (transaction_nonce > on_chain_nonce).then(|| Self::new(sender, transaction_nonce - 1))
    }

    /// Returns the [`TransactionId`] that would come before this transaction.
    pub fn unchecked_ancestor(&self) -> Option<Self> {
        // SAFETY: self.nonce != 0 ⇒ self.nonce >= 1
        (self.nonce != 0).then(|| Self::new(self.sender, self.nonce - 1))
    }

    /// Returns the [`TransactionId`] that directly follows this transaction: `self.nonce + 1`
    pub const fn descendant(&self) -> Self {
        Self::new(self.sender, self.next_nonce())
    }

    /// Returns the nonce that follows immediately after this one.
    #[inline]
    pub const fn next_nonce(&self) -> u64 {
        self.nonce + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn test_transaction_id_new() {
        let sender = SenderId(1);
        let tx_id = TransactionId::new(sender, 5);
        assert_eq!(tx_id.sender, sender);
        assert_eq!(tx_id.nonce, 5);
    }

    #[test]
    fn test_transaction_id_ancestor() {
        let sender = SenderId(1);

        // Special case with nonce 0 and higher on-chain nonce
        let tx_id = TransactionId::ancestor(0, 1, sender);
        assert_eq!(tx_id, None);

        // Special case with nonce 0 and same on-chain nonce
        let tx_id = TransactionId::ancestor(0, 0, sender);
        assert_eq!(tx_id, None);

        // Ancestor is the previous nonce if the transaction nonce is higher than the on-chain nonce
        let tx_id = TransactionId::ancestor(5, 0, sender);
        assert_eq!(tx_id, Some(TransactionId::new(sender, 4)));

        // No ancestor if the transaction nonce is the same as the on-chain nonce
        let tx_id = TransactionId::ancestor(5, 5, sender);
        assert_eq!(tx_id, None);

        // No ancestor if the transaction nonce is lower than the on-chain nonce
        let tx_id = TransactionId::ancestor(5, 15, sender);
        assert_eq!(tx_id, None);
    }

    #[test]
    fn test_transaction_id_unchecked_ancestor() {
        let sender = SenderId(1);

        // Ancestor is the previous nonce if transaction nonce is higher than 0
        let tx_id = TransactionId::new(sender, 5);
        assert_eq!(tx_id.unchecked_ancestor(), Some(TransactionId::new(sender, 4)));

        // No ancestor if transaction nonce is 0
        let tx_id = TransactionId::new(sender, 0);
        assert_eq!(tx_id.unchecked_ancestor(), None);
    }

    #[test]
    fn test_transaction_id_descendant() {
        let sender = SenderId(1);
        let tx_id = TransactionId::new(sender, 5);
        let descendant = tx_id.descendant();
        assert_eq!(descendant, TransactionId::new(sender, 6));
    }

    #[test]
    fn test_transaction_id_next_nonce() {
        let sender = SenderId(1);
        let tx_id = TransactionId::new(sender, 5);
        assert_eq!(tx_id.next_nonce(), 6);
    }

    #[test]
    fn test_transaction_id_ord_eq_sender() {
        let tx1 = TransactionId::new(100u64.into(), 0u64);
        let tx2 = TransactionId::new(100u64.into(), 1u64);
        assert!(tx2 > tx1);
        let set = BTreeSet::from([tx1, tx2]);
        assert_eq!(set.into_iter().collect::<Vec<_>>(), vec![tx1, tx2]);
    }

    #[test]
    fn test_transaction_id_ord() {
        let tx1 = TransactionId::new(99u64.into(), 0u64);
        let tx2 = TransactionId::new(100u64.into(), 1u64);
        assert!(tx2 > tx1);
        let set = BTreeSet::from([tx1, tx2]);
        assert_eq!(set.into_iter().collect::<Vec<_>>(), vec![tx1, tx2]);
    }

    #[test]
    fn test_address_retrieval() {
        let mut identifiers = SenderIdentifiers::default();
        let address = Address::new([1; 20]);
        let id = identifiers.sender_id_or_create(address);
        assert_eq!(identifiers.address(&id), Some(&address));
    }

    #[test]
    fn test_sender_id_retrieval() {
        let mut identifiers = SenderIdentifiers::default();
        let address = Address::new([1; 20]);
        let id = identifiers.sender_id_or_create(address);
        assert_eq!(identifiers.sender_id(&address), Some(id));
    }

    #[test]
    fn test_sender_id_or_create_existing() {
        let mut identifiers = SenderIdentifiers::default();
        let address = Address::new([1; 20]);
        let id1 = identifiers.sender_id_or_create(address);
        let id2 = identifiers.sender_id_or_create(address);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_sender_id_or_create_new() {
        let mut identifiers = SenderIdentifiers::default();
        let address1 = Address::new([1; 20]);
        let address2 = Address::new([2; 20]);
        let id1 = identifiers.sender_id_or_create(address1);
        let id2 = identifiers.sender_id_or_create(address2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_next_id_wrapping() {
        let mut identifiers = SenderIdentifiers { id: u64::MAX, ..Default::default() };

        // The current ID is `u64::MAX`, the next ID should wrap around to 0.
        let id1 = identifiers.next_id();
        assert_eq!(id1, SenderId(u64::MAX));

        // The next ID should now be 0 because of wrapping.
        let id2 = identifiers.next_id();
        assert_eq!(id2, SenderId(0));

        // And then 1, continuing incrementing.
        let id3 = identifiers.next_id();
        assert_eq!(id3, SenderId(1));
    }

    #[test]
    fn test_sender_id_start_bound() {
        let sender = SenderId(1);
        let start_bound = sender.start_bound();
        if let std::ops::Bound::Included(tx_id) = start_bound {
            assert_eq!(tx_id, TransactionId::new(sender, 0));
        } else {
            panic!("Expected included bound");
        }
    }

    #[test]
    fn test_sender_slot_map_insert_get_remove() {
        let mut map = SenderSlotMap::default();
        let id0 = SenderId(0);
        let id5 = SenderId(5);

        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        map.insert(id0, 10u32);
        map.insert(id5, 50u32);

        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&id0), Some(&10));
        assert_eq!(map.get(&id5), Some(&50));
        assert_eq!(map.get(&SenderId(3)), None);
        assert!(map.contains_key(&id0));
        assert!(!map.contains_key(&SenderId(3)));

        // overwrite
        map.insert(id0, 11);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&id0), Some(&11));

        // remove
        assert_eq!(map.remove(&id0), Some(11));
        assert_eq!(map.len(), 1);
        assert!(!map.contains_key(&id0));

        // remove non-existent
        assert_eq!(map.remove(&SenderId(99)), None);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_sender_slot_map_values_and_iter() {
        let mut map = SenderSlotMap::default();
        map.insert(SenderId(1), "a");
        map.insert(SenderId(3), "b");
        map.insert(SenderId(5), "c");

        let values: Vec<_> = map.values().collect();
        assert_eq!(values, vec![&"a", &"b", &"c"]);

        let pairs: Vec<_> = map.iter().collect();
        assert_eq!(pairs, vec![(SenderId(1), &"a"), (SenderId(3), &"b"), (SenderId(5), &"c")]);
    }

    #[test]
    fn test_sender_slot_map_get_or_insert_default() {
        let mut map = SenderSlotMap::<usize>::default();
        let id = SenderId(2);
        *map.get_or_insert_default(id) += 1;
        *map.get_or_insert_default(id) += 1;
        assert_eq!(map.get(&id), Some(&2));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_sender_slot_map_clear() {
        let mut map = SenderSlotMap::default();
        map.insert(SenderId(0), 1);
        map.insert(SenderId(1), 2);
        assert_eq!(map.len(), 2);
        map.clear();
        assert!(map.is_empty());
        assert_eq!(map.get(&SenderId(0)), None);
    }
}
