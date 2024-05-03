use std::{
    borrow, fmt,
    hash::{Hash, Hasher},
};

#[derive(Debug)]
pub struct Table<const N: usize, A, B>
where
    A: Hashed + PartialEq + fmt::Debug,
    B: Default + fmt::Debug,
{
    store: [Entry<A, B>; N],
    collisions: usize,
}

#[derive(Copy, Clone, Debug)]
struct Entry<A, B>
where
    A: Hashed + PartialEq + fmt::Debug,
    B: Default + fmt::Debug,
{
    key: Option<A>,
    value: B,
}

impl<const N: usize, A, B> Default for Table<N, A, B> 
where
    A: Hashed + PartialEq + fmt::Debug + Copy,
    B: Default + fmt::Debug + Copy,
{
    fn default() -> Self {
        Table::new()
    }
}

impl<A, B> Default for Entry<A, B>
where
    A: Hashed + PartialEq + fmt::Debug,
    B: Default + fmt::Debug,
{
    fn default() -> Self {
        Self {
            key: None,
            value: Default::default(),
        }
    }
}

impl<const N: usize, A, B> Table<N, A, B>
where
    A: Hashed + Copy + PartialEq + fmt::Debug,
    B: Default + Copy + fmt::Debug,
{
    pub fn new() -> Self {
        Self {
            store: [Entry::default(); N],
            collisions: 0,
        }
    }

    pub fn insert(&mut self, key: A, value: B) {
        let hash = key.compute_hash();
        let mut index = hash % N;
        loop {
            let e = &mut self.store[index];
            if e.key.is_some_and(|k| k != key) {
                // Fine better functions
                index = (index + hash.reverse_bits()) % N;
            } else {
                e.key = Some(key);
                e.value = value;
                break;
            }
        }
    }

    pub fn get<K>(&self, key: &K) -> Option<&B>
    where
        A: borrow::Borrow<K>,
        K: Hashed + Eq + fmt::Debug,
    {
        let hash = key.compute_hash();
        let mut index = hash % N;
        loop {
            let e = &self.store[index];
            if let Some(k) = e.key {
                if k.borrow() == key {
                    break Some(&e.value);
                } else {
                    break None;
                }
            } else {
                index = (index + hash.reverse_bits()) % N;
             }
        }
    }

    pub fn get_mut<K>(&mut self, key: &K) -> Option<&mut B>
    where
        A: borrow::Borrow<K>,
        K: Hashed + Eq,
    {
        let hash = key.compute_hash();
        let mut index = hash % N;
        loop {
            let e = &self.store[index];
            if let Some(k) = e.key {
                if k.borrow() == key {
                    break Some(&mut self.store[index].value);
                } else {
                    index = (index + hash.reverse_bits()) % N;
                }
            } else {
                break None;
            }
        }
    }

    #[inline]
    pub fn emplace(&mut self, key: A) -> &mut B {
        let hash = key.compute_hash();
        let mut index = hash % N;
        loop {
            if let Some(k) = &self.store[index].key {
                if k == &key {
                    break &mut self.store[index].value;
                } else {
                    self.collisions += 1;
                    index = (index + hash.reverse_bits()) % N;
                }
            } else {
                self.store[index].key = Some(key);
                break &mut self.store[index].value;
            }
        }
    }

    pub fn collision_count(&self) -> usize {
        self.collisions
    }

    pub fn iter(&self) -> TableIterator<N, A, B> {
        TableIterator {
            inner: self,
            index: 0,
        }
    }
}

pub struct TableIterator<'a, const N: usize, A, B>
where
    A: Hashed + Copy + PartialEq + fmt::Debug,
    B: Default + Copy + fmt::Debug,
{
    inner: &'a Table<N, A, B>,
    index: usize,
}

impl<'a, const N: usize, A, B> Iterator for TableIterator<'a, N, A, B>
where
    A: Hashed + Copy + PartialEq + fmt::Debug,
    B: Default + Copy + fmt::Debug,
{
    type Item = (A, B);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let store = &self.inner.store;
        let mut return_value = None;
        while self.index < store.len() {
            if let Some(key) = store[self.index].key {
                return_value = Some((key, store[self.index].value));
                self.index += 1;
                break;
            } else {
                self.index += 1;
            }
        }

        return_value
    }
}

pub trait Hashed {
    fn compute_hash(&self) -> usize;
}

impl<'a> Hashed for &'a str {
    #[inline]
    fn compute_hash(&self) -> usize {
        let mut hasher = rustc_hash::FxHasher::default();
        self.hash(&mut hasher);
        hasher.finish() as usize
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn testies() {
        let mut h = super::Table::<419, &str, i32>::new();
        println!(" Wut? ");
        h.insert("Paudrigue Anderzorn", 46);
        h.insert("Sanna Japp", 38);
        h.insert("Zum-Zum", 6);
        h.insert("Balooo", 62);
        assert_eq!(Some(&46), h.get(&"Paudrigue Anderzorn"));
        assert_eq!(Some(&38), h.get(&"Sanna Japp"));

        if let Some(e) = h.get_mut(&"Paudrigue Anderzorn") {
            *e = 47;
        }

        assert_eq!(Some(&47), h.get(&"Paudrigue Anderzorn"));
        assert_eq!(Some(&6), h.get(&"Zum-Zum"));
        assert_eq!(Some(&62), h.get(&"Balooo"));

        *h.emplace("Sanna Japp") += 1;
        assert_eq!(Some(&39), h.get(&"Sanna Japp"));

        for (key, value) in h.iter() {
            println!("{key} {value}"); 
        }
    }
}
