# btrfs-send-go

Some very basic starting code with the hope of improving Docker's btrfs driver so that it doesn't have to diff the entire tree.

I got as far as making a go program to understand the output of the syscall, but then when I tried to wire it into Docker I hit a wall where I couldn't understand what should go where, and the project petered out.

mbideau thankfully continued my work into a [full-fledged GPL3-licensed diff utility for snapshots](https://github.com/mbideau/btrfs-diff-go). Thanks! You probably want to look there instead, unless you're looking for something with a more permissive license!
