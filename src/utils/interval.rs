pub struct Interval<T> {
    pub lo: T,
    pub hi: T,
}

impl<T: PartialOrd + Clone> Interval<T> {
    pub fn is_empty(&self) -> bool {
        return self.lo > self.hi;
    }

    pub fn expand(&mut self, val: &T) {
        if val > &self.hi {
            self.hi = val.clone();
        }
        if val < &self.lo {
            self.lo = val.clone();
        }
    }

    pub fn overlaps(&self, other: &Interval<T>) -> bool {
        return self.contains(&other.lo) || self.contains(&other.hi);
    }
    pub fn contains(&self, p: &T) -> bool {
        return &self.lo <= p && p <= &self.hi;
    }
}
