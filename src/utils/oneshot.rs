pub struct Receiver<T> {
    receiver: std::sync::mpsc::Receiver<T>,
}
pub struct Sender<T> {
    sender: std::sync::mpsc::Sender<T>,
}

impl<T> Receiver<T> {
    pub fn recv(self) -> Result<T, std::sync::mpsc::RecvError> {
        self.receiver.recv()
    }
}

impl<T> Sender<T> {
    pub fn send(self, item: T) -> Result<(), std::sync::mpsc::SendError<T>> {
        self.sender.send(item)
    }
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (sender, receiver) = std::sync::mpsc::channel();
    (Sender { sender }, Receiver { receiver })
}
