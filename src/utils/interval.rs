

struct Interval<T> {
 lo: T,
 hi: T,
}

impl<T :PartialOrd> Interval<T> {
    pub fn overlaps (&self, other: &Interval<T>) -> bool {
        return self.contains(&other.lo) || self.contains(&other.hi);
    }
    pub fn contains(&self, p: &T) -> bool {
        return &self.lo <= p && p <= &self.hi;
    }
}