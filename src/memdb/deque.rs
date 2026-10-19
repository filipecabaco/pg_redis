//! A double-ended list as two stacks back to back: `front` holds the head in
//! reverse, `back` the tail in order. Pushes and pops at either end are O(1)
//! amortised, indexing is O(1), and it needs nothing past `Vec` — the
//! allocator-aware collections stop there.
#![forbid(unsafe_code)]

use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;

pub struct Deque<T, A: Allocator + Clone> {
    front: Vec<T, A>,
    back: Vec<T, A>,
}

impl<T, A: Allocator + Clone> Deque<T, A> {
    pub fn new_in(alloc: A) -> Self {
        Deque {
            front: Vec::new_in(alloc.clone()),
            back: Vec::new_in(alloc),
        }
    }

    pub fn len(&self) -> usize {
        self.front.len() + self.back.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        let f = self.front.len();
        if i < f {
            self.front.get(f - 1 - i)
        } else {
            self.back.get(i - f)
        }
    }

    pub fn get_mut(&mut self, i: usize) -> Option<&mut T> {
        let f = self.front.len();
        if i < f {
            self.front.get_mut(f - 1 - i)
        } else {
            self.back.get_mut(i - f)
        }
    }

    pub fn push_front(&mut self, v: T) {
        self.front.push(v);
    }

    pub fn push_back(&mut self, v: T) {
        self.back.push(v);
    }

    pub fn pop_front(&mut self) -> Option<T> {
        if self.front.is_empty() {
            Self::rebalance(&mut self.back, &mut self.front);
        }
        self.front.pop()
    }

    pub fn pop_back(&mut self) -> Option<T> {
        if self.back.is_empty() {
            Self::rebalance(&mut self.front, &mut self.back);
        }
        self.back.pop()
    }

    /// Refill the empty stack `to` with the half of `from` nearest its end,
    /// so a run of pops from one side stays amortised O(1). `from` is laid
    /// out in the opposite direction, so its first elements are the ones
    /// nearest `to`'s end.
    fn rebalance(from: &mut Vec<T, A>, to: &mut Vec<T, A>) {
        let n = from.len();
        if n == 0 {
            return;
        }
        let take = n.div_ceil(2);
        let rest = from.split_off(take);
        let head = std::mem::replace(from, rest);
        // `head` is in `from`'s direction; `to` wants it reversed.
        to.extend(head.into_iter().rev());
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        self.front.iter().rev().chain(self.back.iter())
    }

    /// Rebuild from `items` in order, replacing what was there.
    pub fn replace_with(&mut self, items: impl IntoIterator<Item = T>) {
        self.front.clear();
        self.back.clear();
        self.back.extend(items);
    }

    /// Insert `v` so it ends up at index `i` (`i <= len`).
    pub fn insert(&mut self, i: usize, v: T) {
        let f = self.front.len();
        if i <= f {
            self.front.insert(f - i, v)
        } else {
            self.back.insert(i - f, v)
        }
    }

    /// Keep only the elements `keep` accepts, given each one's index.
    pub fn retain_indexed(&mut self, mut keep: impl FnMut(usize, &T) -> bool) {
        let alloc = self.back.allocator().clone();
        let old = std::mem::replace(self, Deque::new_in(alloc));
        for (i, v) in old.front.into_iter().rev().chain(old.back).enumerate() {
            if keep(i, &v) {
                self.back.push(v);
            }
        }
    }

    /// Keep elements `start..=stop` (in range, `start <= stop`), drop the rest.
    pub fn trim(&mut self, start: usize, stop: usize) {
        let len = self.len();
        for _ in stop + 1..len {
            self.pop_back();
        }
        for _ in 0..start {
            self.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator_api2::alloc::Global;

    fn collect(d: &Deque<i32, Global>) -> std::vec::Vec<i32> {
        d.iter().copied().collect()
    }

    #[test]
    fn behaves_like_vecdeque_under_mixed_operations() {
        let mut d = Deque::new_in(Global);
        let mut model = std::collections::VecDeque::new();
        let mut x: u32 = 7;
        for step in 0..5000 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            match x % 7 {
                0 | 1 => {
                    d.push_front(step);
                    model.push_front(step);
                }
                2 | 3 => {
                    d.push_back(step);
                    model.push_back(step);
                }
                4 => assert_eq!(d.pop_front(), model.pop_front()),
                5 => assert_eq!(d.pop_back(), model.pop_back()),
                _ if !model.is_empty() => {
                    let i = (x as usize / 7) % (model.len() + 1);
                    d.insert(i, -step);
                    model.insert(i, -step);
                }
                _ => {}
            }
            assert_eq!(d.len(), model.len());
        }
        assert_eq!(
            collect(&d),
            model.iter().copied().collect::<std::vec::Vec<_>>()
        );
        for i in 0..model.len() {
            assert_eq!(d.get(i), model.get(i));
        }
    }

    #[test]
    fn trim_keeps_the_inclusive_range() {
        let mut d = Deque::new_in(Global);
        for i in 0..10 {
            if i % 2 == 0 {
                d.push_back(i)
            } else {
                d.push_front(i)
            }
        }
        let all = collect(&d);
        d.trim(2, 6);
        assert_eq!(collect(&d), all[2..=6]);
    }
}
