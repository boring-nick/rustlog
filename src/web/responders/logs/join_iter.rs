use std::fmt::Display;

pub struct Join<Iter, Sep>(pub Iter, pub Sep);

impl<Iter, Sep> Display for Join<Iter, Sep>
where
    // TODO: get rid of this `Clone` bound by doing `peek`
    // manually
    Iter: Iterator + Clone,
    <Iter as Iterator>::Item: Display,
    Sep: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sep = &self.1;
        let mut peekable = self.0.clone().peekable();
        while let Some(item) = peekable.next() {
            write!(f, "{item}")?;
            if peekable.peek().is_some() {
                write!(f, "{sep}")?;
            }
        }
        Ok(())
    }
}

pub trait JoinIter: Sized {
    fn join<Sep>(&self, sep: Sep) -> Join<Self, Sep>;
}

impl<Iter> JoinIter for Iter
where
    Iter: Sized + Iterator + Clone,
{
    fn join<Sep>(&self, sep: Sep) -> Join<Self, Sep> {
        Join(self.clone(), sep)
    }
}
