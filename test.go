package main

// We get the constants from this header.

// #include <btrfs/send.h>
// #include <btrfs/send-utils.h>
// #include <btrfs/ioctl.h>
// #cgo LDFLAGS: -lbtrfs
import "C"

import "os"
import "io"
import "encoding/binary"
import "bufio"
import "fmt"
import "unsafe"
import "syscall"
import "strings"

// NAUGHTYNESS:
// For a recursive delete, we get a rename, then a delete on the renamed copy.
// * We need understand that if we rm a renamed path, we should unrename anything inside it for the diff.
// For a create, we get a garbage name, then a rename.
// * We need to understand that if we get a rename of a file that was new, we must rename all the stuff we did to it.

type Op int

const (
	OpUnspec Op = iota
	OpIgnore
	OpCreate
	OpModify
	OpDelete
	OpRename // Special cased -- we need two paths
	OpEnd
)

var names []string = []string{"!!!", "ignored", "added", "changed", "deleted", "renamed", "END"}

func (op Op) String() string {
	return names[op]
}

type CommandSpec struct {
	Name string
	Op   Op
}

type Command struct {
	Type *CommandSpec
	body []byte
}

func initCommands() *[C.__BTRFS_SEND_C_MAX]CommandSpec {
	var commands [C.__BTRFS_SEND_C_MAX]CommandSpec
	commands[C.BTRFS_SEND_C_UNSPEC] = CommandSpec{Name: "BTRFS_SEND_C_UNSPEC", Op: OpUnspec}

	commands[C.BTRFS_SEND_C_SUBVOL] = CommandSpec{Name: "BTRFS_SEND_C_SUBVOL", Op: OpIgnore}
	commands[C.BTRFS_SEND_C_SNAPSHOT] = CommandSpec{Name: "BTRFS_SEND_C_SNAPSHOT", Op: OpIgnore}

	commands[C.BTRFS_SEND_C_MKFILE] = CommandSpec{Name: "BTRFS_SEND_C_MKFILE", Op: OpCreate}
	commands[C.BTRFS_SEND_C_MKDIR] = CommandSpec{Name: "BTRFS_SEND_C_MKDIR", Op: OpCreate}
	commands[C.BTRFS_SEND_C_MKNOD] = CommandSpec{Name: "BTRFS_SEND_C_MKNOD", Op: OpCreate}
	commands[C.BTRFS_SEND_C_MKFIFO] = CommandSpec{Name: "BTRFS_SEND_C_MKFIFO", Op: OpCreate}
	commands[C.BTRFS_SEND_C_MKSOCK] = CommandSpec{Name: "BTRFS_SEND_C_MKSOCK", Op: OpCreate}
	commands[C.BTRFS_SEND_C_SYMLINK] = CommandSpec{Name: "BTRFS_SEND_C_SYMLINK", Op: OpCreate}

	commands[C.BTRFS_SEND_C_RENAME] = CommandSpec{Name: "BTRFS_SEND_C_RENAME", Op: OpRename}
	commands[C.BTRFS_SEND_C_LINK] = CommandSpec{Name: "BTRFS_SEND_C_LINK", Op: OpCreate}
	commands[C.BTRFS_SEND_C_UNLINK] = CommandSpec{Name: "BTRFS_SEND_C_UNLINK", Op: OpDelete}
	commands[C.BTRFS_SEND_C_RMDIR] = CommandSpec{Name: "BTRFS_SEND_C_RMDIR", Op: OpDelete}

	commands[C.BTRFS_SEND_C_SET_XATTR] = CommandSpec{Name: "BTRFS_SEND_C_SET_XATTR", Op: OpModify}
	commands[C.BTRFS_SEND_C_REMOVE_XATTR] = CommandSpec{Name: "BTRFS_SEND_C_REMOVE_XATTR", Op: OpModify}

	commands[C.BTRFS_SEND_C_WRITE] = CommandSpec{Name: "BTRFS_SEND_C_WRITE", Op: OpModify}
	commands[C.BTRFS_SEND_C_CLONE] = CommandSpec{Name: "BTRFS_SEND_C_CLONE", Op: OpModify}

	commands[C.BTRFS_SEND_C_TRUNCATE] = CommandSpec{Name: "BTRFS_SEND_C_TRUNCATE", Op: OpModify}
	commands[C.BTRFS_SEND_C_CHMOD] = CommandSpec{Name: "BTRFS_SEND_C_CHMOD", Op: OpModify}
	commands[C.BTRFS_SEND_C_CHOWN] = CommandSpec{Name: "BTRFS_SEND_C_CHOWN", Op: OpModify}
	commands[C.BTRFS_SEND_C_UTIMES] = CommandSpec{Name: "BTRFS_SEND_C_UTIMES", Op: OpModify}

	commands[C.BTRFS_SEND_C_END] = CommandSpec{Name: "BTRFS_SEND_C_END", Op: OpEnd}
	commands[C.BTRFS_SEND_C_UPDATE_EXTENT] = CommandSpec{Name: "BTRFS_SEND_C_UPDATE_EXTENT", Op: OpModify}
	for i, command := range commands {
		if i != C.BTRFS_SEND_C_UNSPEC && command.Op == OpUnspec {
			return nil
		}
	}
	return &commands
}

var commands *[C.__BTRFS_SEND_C_MAX]CommandSpec = initCommands()

type Node struct {
	Children   map[string]*Node
	Name       string
	ChangeType Op
	Parent     *Node
	Original   *Node
}

type Diff struct {
	Original Node
	New      Node
}

func (diff *Diff) tagPath(path string, changeType Op) {
	fmt.Fprintf(os.Stdout, "TRACE %10v %v\n", changeType, path)
	fileNode := diff.find(path, changeType == OpCreate)
	if changeType == OpDelete {
		if fileNode.Original == nil {
			fmt.Fprintf(os.Stderr, "deleting path %v which was created in same diff?\n", path)
		}
		delete(fileNode.Parent.Children, fileNode.Name)
	} else { // Why this? if fileNode.Original != nil {
		if !(fileNode.ChangeType == OpCreate && changeType == OpModify) {
			fileNode.ChangeType = changeType
		}
	}
	if changeType == OpDelete {
		// If we deleted /this/ node, it sure as hell needs no children.
		fileNode.Children = nil
		if fileNode.Original != nil {
			// Leave behind a sentinel in the Original structure.
			fileNode.Original.ChangeType = OpDelete
			fileNode.Original.verifyDelete(path)
			fileNode.Original.Children = nil
		}
	}
	//fmt.Fprintf(os.Stderr, "intermediate=%v\n", diff)
}

func (node *Node) verifyDelete(path string) {
	for _, child := range node.Children {
		if child.ChangeType != OpDelete && child.ChangeType != OpRename {
			fmt.Fprintf(os.Stderr, "deleting parent of node %v in %v which is not gone", node, path)
		}
	}
}

func (diff *Diff) rename(from string, to string) {
	fmt.Fprintf(os.Stdout, "TRACE %10v %v\n", "rename", from)
	fmt.Fprintf(os.Stdout, "TRACE %10v %v\n", "rename_to", to)
	fromNode := diff.find(from, false)
	delete(fromNode.Parent.Children, fromNode.Name)
	if fromNode.Original != nil {
		// if fromNode had an original, we must mark that path destroyed.
		fromNode.Original.ChangeType = OpRename
	}
	toNode := diff.find(to, true)
	toNode.Parent.Children[toNode.Name] = fromNode
	fromNode.Name = toNode.Name
	fromNode.ChangeType = OpCreate
	fromNode.Parent = toNode.Parent
	//fmt.Fprintf(os.Stderr, "intermediate=%v\n", diff)
}

func (diff *Diff) find(path string, isNew bool) *Node {
	if diff.New.Original == nil {
		diff.New.Original = &diff.Original
	}
	if path == "" {
		return &diff.New
	}
	parts := strings.Split(path, "/")
	current := &diff.New
	for i, part := range parts {
		if current.Children == nil {
			current.Children = make(map[string]*Node)
		}
		newNode := current.Children[part]
		if newNode == nil {
			current.Children[part] = &Node{}
			newNode = current.Children[part]
			original := current.Original
			if original == nil {
				if !(isNew && i == len(parts)-1) {
					// Either a path has a route in the original, or it's been
					// explicitly created. Once we traverse into a path without
					// an original, we know the full tree, so getting here is a
					// sign we did it wrong.
					fmt.Fprintf(os.Stderr, "referenced path %v cannot exist\n", path)
					os.Exit(1)
				}
			} else {
				if original.Children == nil {
					original.Children = make(map[string]*Node)
				}
				newOriginal := original.Children[part]
				if newOriginal == nil {
					if !isNew || i < len(parts)-1 {
						fmt.Fprintf(os.Stderr, "ACK %v %v %v %v %v\n", original, isNew, path, part, newOriginal)
						// Was meant to already exist, so make sure it did!
						original.Children[part] = &Node{}
						newOriginal = original.Children[part]
						newOriginal.Name = part
						newOriginal.Parent = original
						newNode.Original = newOriginal
					}
				}
			}
			newNode.Name = part
			newNode.Parent = current
		} else if isNew && i == len(parts)-1 {
			// As this is the target of a create, we should expect to see
			// nothing here.
			fmt.Fprintf(os.Stderr, "overwritten path %v already existed\n", path)
		}
		current = newNode
	}
	return current
}

func (node *Node) String() string {
	return fmt.Sprintf("(%v, %v, %v)", node.Children, node.ChangeType, node.Name)
}

func (diff *Diff) String() string {
	return "\n\t" + strings.Join((diff.Changes())[:], "\n\t") + "\n"
}

func (diff *Diff) Changes() []string {
	newFiles := make(map[string]*Node)
	oldFiles := make(map[string]*Node)
	changes(&diff.New, "", newFiles)
	changes(&diff.Original, "", oldFiles)
	fmt.Fprintf(os.Stderr, "new: %v\n%v\n", newFiles, &diff.New)
	fmt.Fprintf(os.Stderr, "old: %v\n%v\n", oldFiles, &diff.Original)
	var ret []string
	for name, node := range oldFiles {
		if newFiles[name] != nil && node.ChangeType == OpUnspec {
			if node.Children == nil {
				// TODO diff equality only
				ret = append(ret, fmt.Sprintf("%10v: %v", OpModify, name))
			}
			delete(newFiles, name)
		} else {
			if node.ChangeType != OpDelete && node.ChangeType != OpRename {
				fmt.Fprintf(os.Stderr, "unexpected ChangeType on original %v: %v", name, node.ChangeType)
			}
			if (node.ChangeType == OpDelete || node.ChangeType == OpRename) && newFiles[name] != nil && newFiles[name].ChangeType == OpCreate {
				ret = append(ret, fmt.Sprintf("%10v: %v", OpModify, name))
				delete(newFiles, name)
			} else {
				//fmt.Fprintf(os.Stderr, "DEBUG DEBUG %v %v %v\n ", node.ChangeType, newFiles[name], name)
				ret = append(ret, fmt.Sprintf("%10v: %v", node.ChangeType, name))
			}
		}
	}
	for name := range newFiles {
		ret = append(ret, fmt.Sprintf("%10v: %v", OpCreate, name))
	}
	return ret
}

func changes(node *Node, prefix string, ret map[string]*Node) {
	newPrefix := prefix + node.Name
	ret[newPrefix] = node
	if node.ChangeType == OpCreate {
		// TODO diff equality only
		return
	}
	for _, child := range node.Children {
		changes(child, newPrefix+"/", ret)
	}
}

func peekAndDiscard(input *bufio.Reader, n int) ([]byte, error) {
	data, err := input.Peek(n)
	if err != nil {
		return nil, err
	}
	if _, err := input.Discard(n); err != nil {
		return nil, err
	}
	return data, nil
}

func readCommand(input *bufio.Reader) (*Command, error) {
	cmdSizeB, err := peekAndDiscard(input, 4)
	if err != nil {
		return nil, fmt.Errorf("Short read on command size: %v", err)
	}
	cmdTypeB, err := peekAndDiscard(input, 2)
	if err != nil {
		return nil, fmt.Errorf("Short read on command type: %v", err)
	}
	if _, err := peekAndDiscard(input, 4); err != nil {
		return nil, fmt.Errorf("Short read on command checksum: %v", err)
	}
	cmdSize := binary.LittleEndian.Uint32(cmdSizeB)
	cmdData, err := peekAndDiscard(input, int(cmdSize))
	if err != nil {
		return nil, fmt.Errorf("Short read on command body: %v", err)
	}
	cmdType := binary.LittleEndian.Uint16(cmdTypeB)
	if cmdType < 0 || cmdType > C.BTRFS_SEND_C_MAX {
		return nil, fmt.Errorf("Stream contains invalid command type %v", cmdType)
	}
	fmt.Fprintf(os.Stdout, "Cmd %v; type %v\n", cmdData, cmdType)
	return &Command{
		Type: &commands[cmdType],
		body: cmdData,
	}, nil
}

func (command *Command) ReadParam(expectedType int) (string, error) {
	if len(command.body) < 4 {
		return "", fmt.Errorf("No more parameters")
	}
	paramType := binary.LittleEndian.Uint16(command.body[0:2])
	if int(paramType) != expectedType {
		return "", fmt.Errorf("Expect type %v; got %v", expectedType, paramType)
	}
	paramLength := binary.LittleEndian.Uint16(command.body[2:4])
	if int(paramLength)+4 > len(command.body) {
		return "", fmt.Errorf("Short command param; length was %v but only %v left", paramLength, len(command.body)-4)
	}
	ret := string(command.body[4 : 4+paramLength])
	command.body = command.body[4+paramLength:]
	return ret, nil
}

func readStream(stream io.Reader, diff *Diff, channel chan error) {
	input := bufio.NewReader(stream)
	btrfsStreamHeader, err := input.ReadString('\x00')
	if err != nil {
		channel <- err
		return
	}
	if btrfsStreamHeader[:len(btrfsStreamHeader)-1] != C.BTRFS_SEND_STREAM_MAGIC {
		channel <- fmt.Errorf("magic is %v, not %v", btrfsStreamHeader, C.BTRFS_SEND_STREAM_MAGIC)
		return
	}
	ver, err := peekAndDiscard(input, 4)
	if err != nil || binary.LittleEndian.Uint32(ver) != 1 {
		channel <- err
		return
	}
	for true {
		command, err := readCommand(input)
		if err != nil {
			channel <- err
			return
		}
		if command.Type.Op == OpUnspec {
			channel <- err
			return
		} else if command.Type.Op == OpIgnore {
			continue
		} else if command.Type.Op == OpRename {
			fromPath, err := command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				channel <- err
				return
			}
			toPath, err := command.ReadParam(C.BTRFS_SEND_A_PATH_TO)
			if err != nil {
				channel <- err
				return
			}
			diff.rename(fromPath, toPath)
		} else if command.Type.Op == OpEnd {
			fmt.Fprintf(os.Stderr, "END\n")
			channel <- nil
			break
		} else {
			path, err := command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				channel <- err
				return
			}
			diff.tagPath(path, command.Type.Op)
		}
	}
}

func main() {
	read, write, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipe returned %v\n", err)
		os.Exit(1)
	}

	root := "/disks/ssdbtrfs"
	parent := os.Args[1]
	child := os.Args[2]
	root_f, err := os.OpenFile(root, os.O_RDONLY, 0777)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open returned %v\n", err)
		os.Exit(1)
	}
	subvol_f, err := os.OpenFile(child, os.O_RDONLY, 0777)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open returned %v\n", err)
		os.Exit(1)
	}
	var sus C.struct_subvol_uuid_search
	var subvol_info *C.struct_subvol_info
	r := C.subvol_uuid_search_init(C.int(root_f.Fd()), &sus)
	if r < 0 {
		fmt.Fprintf(os.Stderr, "subvol_uuid_search_init returned %v\n", r)
		os.Exit(1)
	}
	subvol_info, err = C.subvol_uuid_search(&sus, 0, nil, 0, C.CString(parent), C.subvol_search_by_path)
	if subvol_info == nil {
		fmt.Fprintf(os.Stderr, "subvol_uuid_search returned %v\n", err)
		os.Exit(1)
	}
	var root_id C.__u64 = C.__u64(subvol_info.root_id)
	fmt.Fprintf(os.Stderr, "root_id %v\n", root_id)
	//subvol_info = C.subvol_uuid_search(&sus, root_id, nil, 0, nil, C.subvol_search_by_root_id)
	var subvol_fd C.uint = C.uint(subvol_f.Fd())

	var opts C.struct_btrfs_ioctl_send_args
	opts.send_fd = C.__s64(write.Fd())
	opts.clone_sources = &root_id
	opts.clone_sources_count = 1
	opts.parent_root = root_id
	opts.flags = C.BTRFS_SEND_FLAG_NO_FILE_DATA
	channel := make(chan error)
	var diff Diff = Diff{}
	go readStream(read, &diff, channel)
	r1, r2, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(subvol_fd), C.BTRFS_IOC_SEND, uintptr(unsafe.Pointer(&opts)))
	fmt.Fprintf(os.Stderr, "ioctl returns %v %v %v\n", r1, r2, err)
	err = <-channel
	if err != nil {
		fmt.Fprintf(os.Stderr, "readStream returns %v\n", err)
		os.Exit(1)
	}

	//ret = C.btrfs_read_and_process_send_stream(C.int(os.Stdin.Fd()), &send_ops, unsafe.Pointer(&count), 1, 10)
	//ret = C.btrfs_read_and_process_send_stream(C.int(os.Stdin.Fd()), &send_ops, unsafe.Pointer(&count), 1, 10)
	//ret = C.btrfs_read_and_process_send_stream(C.int(os.Stdin.Fd()), &send_ops, unsafe.Pointer(&count), 1, 10)
	//if ret < 0 {
	//	fmt.Fprintf(os.Stderr, "btrfs_read_and_process_send_stream returned %v\n", ret)
	//	os.Exit(1)
	//}
	fmt.Fprintf(os.Stdout, "TRACE GENERATED\nTRACE %v\n", strings.Join(diff.Changes(), "\nTRACE "))
}
