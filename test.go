package main

// #include <sys/ioctl.h>
// #include <btrfs/send-stream.h>
// #include <btrfs/send-utils.h>
// #include <fcntl.h>
// #cgo LDFLAGS: -lbtrfs
// //BEGIN </tmp/btrfs_hdr perl -ne '/^\tint/ || next; /\tint \(\*(.*)\)\((.*)\);/; ($n, $a) = ($1, $2); $o="extern int cb_$n($a);"; $o =~ s/const //g; $o .= "\nstatic int cb_$n"."1($a) {\n"; $a =~ s/(^|, )const ((?:struct )?\w+)(?:( \*)?| )(\w+)(?=,|$)/$1($2$3)$4/g; $a =~ s/(^|, )((?:struct )?\w+)(?:( \*)?| )(\w+)(?=,|$)/$1$4/g; $o .= "\treturn cb_$n($a);\n}\n\n"; print "$o";'|sed 's/^/\/\/ /'
// extern int cb_subvol(char *path, u8 *uuid, u64 ctransid, void *user);
// static int cb_subvol1(const char *path, const u8 *uuid, u64 ctransid, void *user) {
// 	return cb_subvol((char *)path, (u8 *)uuid, ctransid, user);
// }
//
// extern int cb_snapshot(char *path, u8 *uuid, u64 ctransid, u8 *parent_uuid, u64 parent_ctransid, void *user);
// static int cb_snapshot1(const char *path, const u8 *uuid, u64 ctransid, const u8 *parent_uuid, u64 parent_ctransid, void *user) {
// 	return cb_snapshot((char *)path, (u8 *)uuid, ctransid, (u8 *)parent_uuid, parent_ctransid, user);
// }
//
// extern int cb_mkfile(char *path, void *user);
// static int cb_mkfile1(const char *path, void *user) {
// 	return cb_mkfile((char *)path, user);
// }
//
// extern int cb_mkdir(char *path, void *user);
// static int cb_mkdir1(const char *path, void *user) {
// 	return cb_mkdir((char *)path, user);
// }
//
// extern int cb_mknod(char *path, u64 mode, u64 dev, void *user);
// static int cb_mknod1(const char *path, u64 mode, u64 dev, void *user) {
// 	return cb_mknod((char *)path, mode, dev, user);
// }
//
// extern int cb_mkfifo(char *path, void *user);
// static int cb_mkfifo1(const char *path, void *user) {
// 	return cb_mkfifo((char *)path, user);
// }
//
// extern int cb_mksock(char *path, void *user);
// static int cb_mksock1(const char *path, void *user) {
// 	return cb_mksock((char *)path, user);
// }
//
// extern int cb_symlink(char *path, char *lnk, void *user);
// static int cb_symlink1(const char *path, const char *lnk, void *user) {
// 	return cb_symlink((char *)path, (char *)lnk, user);
// }
//
// extern int cb_rename(char *from, char *to, void *user);
// static int cb_rename1(const char *from, const char *to, void *user) {
// 	return cb_rename((char *)from, (char *)to, user);
// }
//
// extern int cb_link(char *path, char *lnk, void *user);
// static int cb_link1(const char *path, const char *lnk, void *user) {
// 	return cb_link((char *)path, (char *)lnk, user);
// }
//
// extern int cb_unlink(char *path, void *user);
// static int cb_unlink1(const char *path, void *user) {
// 	return cb_unlink((char *)path, user);
// }
//
// extern int cb_rmdir(char *path, void *user);
// static int cb_rmdir1(const char *path, void *user) {
// 	return cb_rmdir((char *)path, user);
// }
//
// extern int cb_write(char *path, void *data, u64 offset, u64 len, void *user);
// static int cb_write1(const char *path, const void *data, u64 offset, u64 len, void *user) {
// 	return cb_write((char *)path, (void *)data, offset, len, user);
// }
//
// extern int cb_clone(char *path, u64 offset, u64 len, u8 *clone_uuid, u64 clone_ctransid, char *clone_path, u64 clone_offset, void *user);
// static int cb_clone1(const char *path, u64 offset, u64 len, const u8 *clone_uuid, u64 clone_ctransid, const char *clone_path, u64 clone_offset, void *user) {
// 	return cb_clone((char *)path, offset, len, (u8 *)clone_uuid, clone_ctransid, (char *)clone_path, clone_offset, user);
// }
//
// extern int cb_set_xattr(char *path, char *name, void *data, int len, void *user);
// static int cb_set_xattr1(const char *path, const char *name, const void *data, int len, void *user) {
// 	return cb_set_xattr((char *)path, (char *)name, (void *)data, len, user);
// }
//
// extern int cb_remove_xattr(char *path, char *name, void *user);
// static int cb_remove_xattr1(const char *path, const char *name, void *user) {
// 	return cb_remove_xattr((char *)path, (char *)name, user);
// }
//
// extern int cb_truncate(char *path, u64 size, void *user);
// static int cb_truncate1(const char *path, u64 size, void *user) {
// 	return cb_truncate((char *)path, size, user);
// }
//
// extern int cb_chmod(char *path, u64 mode, void *user);
// static int cb_chmod1(const char *path, u64 mode, void *user) {
// 	return cb_chmod((char *)path, mode, user);
// }
//
// extern int cb_chown(char *path, u64 uid, u64 gid, void *user);
// static int cb_chown1(const char *path, u64 uid, u64 gid, void *user) {
// 	return cb_chown((char *)path, uid, gid, user);
// }
//
// extern int cb_utimes(char *path, struct timespec *at, struct timespec *mt, struct timespec *ct, void *user);
// static int cb_utimes1(const char *path, struct timespec *at, struct timespec *mt, struct timespec *ct, void *user) {
// 	return cb_utimes((char *)path, at, mt, ct, user);
// }
//
// extern int cb_update_extent(char *path, u64 offset, u64 len, void *user);
// static int cb_update_extent1(const char *path, u64 offset, u64 len, void *user) {
// 	return cb_update_extent((char *)path, offset, len, user);
// }
// //END paste
// static void setup(struct btrfs_send_ops *ops) {
// //BEGIN </tmp/btrfs_hdr perl -ne '/^\tint/ || next; s/\tint \(\*(.*)\)\((.*)\);/\tops->$1 = \&cb_${1}1;/; print'|sed 's/^/\/\/ /'
// 	ops->subvol = &cb_subvol1;
// 	ops->snapshot = &cb_snapshot1;
// 	ops->mkfile = &cb_mkfile1;
// 	ops->mkdir = &cb_mkdir1;
// 	ops->mknod = &cb_mknod1;
// 	ops->mkfifo = &cb_mkfifo1;
// 	ops->mksock = &cb_mksock1;
// 	ops->symlink = &cb_symlink1;
// 	ops->rename = &cb_rename1;
// 	ops->link = &cb_link1;
// 	ops->unlink = &cb_unlink1;
// 	ops->rmdir = &cb_rmdir1;
// 	ops->write = &cb_write1;
// 	ops->clone = &cb_clone1;
// 	ops->set_xattr = &cb_set_xattr1;
// 	ops->remove_xattr = &cb_remove_xattr1;
// 	ops->truncate = &cb_truncate1;
// 	ops->chmod = &cb_chmod1;
// 	ops->chown = &cb_chown1;
// 	ops->utimes = &cb_utimes1;
// 	ops->update_extent = &cb_update_extent1;
// //END paste
// }
import "C"

import "os"
import "fmt"
import "unsafe"
import "syscall"
import "strings"

// NAUGHTYNESS:
// For a recursive delete, we get a rename, then a delete on the renamed copy.
// * We need understand that if we rm a renamed path, we should unrename anything inside it for the diff.
// For a create, we get a garbage name, then a rename.
// * We need to understand that if we get a rename of a file that was new, we must rename all the stuff we did to it.

// BEGIN </tmp/btrfs_hdr perl -ne '/^\tint/ || next; s/\tint \(\*(.*)\)\((.*)\);/\/\/export cb_$1\nfunc cb_$1($2) (int) {\n\treturn 0\n}\n/; s/const //g; s/\b(char|u64|int|u8|void)\b/C.$1/g; s/struct (\S+)/C.struct_$1/g; s/([( ])(C\.\S+) (\*?)(\S+)([,)])/$1$4 $3$2$5/g; print'|sed 's/\*C\.void/unsafe.Pointer/g'
//export cb_subvol
func cb_subvol(path *C.char, uuid *C.u8, ctransid C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "subvol: %v\n", C.GoString(path))
	return 0
}

//export cb_snapshot
func cb_snapshot(path *C.char, uuid *C.u8, ctransid C.u64, parent_uuid *C.u8, parent_ctransid C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "snapshot: %v\n", C.GoString(path))
	return 0
}

//export cb_mkfile
func cb_mkfile(path *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "mkfile: %v\n", C.GoString(path))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "added")
	return 0
}

//export cb_mkdir
func cb_mkdir(path *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "mkdir: %v\n", C.GoString(path))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "added")
	return 0
}

//export cb_mknod
func cb_mknod(path *C.char, mode C.u64, dev C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "mknod: %v %v %v\n", C.GoString(path), mode, dev)
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "added")
	return 0
}

//export cb_mkfifo
func cb_mkfifo(path *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "mkfifo: %v\n", C.GoString(path))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "added")
	return 0
}

//export cb_mksock
func cb_mksock(path *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "mksock: %v\n", C.GoString(path))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "added")
	return 0
}

//export cb_symlink
func cb_symlink(path *C.char, lnk *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "symlink: %v %v\n", C.GoString(path), C.GoString(lnk))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "added")
	return 0
}

//export cb_rename
func cb_rename(from *C.char, to *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "rename: %v %v\n", C.GoString(from), C.GoString(to))
	var diff *Diff = (*Diff)(user)
	diff.rename(C.GoString(from), C.GoString(to))
	return 0
}

//export cb_link
func cb_link(path *C.char, lnk *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "link: %v %v\n", C.GoString(path), C.GoString(lnk))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "added")
	return 0
}

//export cb_unlink
func cb_unlink(path *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "unlink: %v\n", C.GoString(path))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "deleted")
	return 0
}

//export cb_rmdir
func cb_rmdir(path *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "rmdir: %v\n", C.GoString(path))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "deleted")
	return 0
}

//export cb_write
func cb_write(path *C.char, data unsafe.Pointer, offset C.u64, len C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "write: %v %v\n", C.GoString(path), len)
	return -1 // Should not happen
}

//export cb_clone
func cb_clone(path *C.char, offset C.u64, len C.u64, clone_uuid *C.u8, clone_ctransid C.u64, clone_path *C.char, clone_offset C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "set_xattr: %v\n", C.GoString(path))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "changed")
	return 0
}

//export cb_set_xattr
func cb_set_xattr(path *C.char, name *C.char, data unsafe.Pointer, len C.int, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "set_xattr: %v %v\n", C.GoString(path), C.GoString(name))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "changed")
	return 0
}

//export cb_remove_xattr
func cb_remove_xattr(path *C.char, name *C.char, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "remove_xattr: %v %v\n", C.GoString(path), C.GoString(name))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "changed")
	return 0
}

//export cb_truncate
func cb_truncate(path *C.char, size C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "truncate: %v %v\n", C.GoString(path), size)
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "changed")
	return 0
}

//export cb_chmod
func cb_chmod(path *C.char, mode C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "chmod: %v %v\n", C.GoString(path), mode)
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "changed")
	return 0
}

//export cb_chown
func cb_chown(path *C.char, uid C.u64, gid C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "chown: %v %v %v\n", C.GoString(path), uid, gid)
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "changed")
	return 0
}

//export cb_utimes
func cb_utimes(path *C.char, at *C.struct_timespec, mt *C.struct_timespec, ct *C.struct_timespec, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "utimes: %v\n", C.GoString(path))
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "changed")
	return 0
}

//export cb_update_extent
func cb_update_extent(path *C.char, offset C.u64, len C.u64, user unsafe.Pointer) C.int {
	fmt.Fprintf(os.Stderr, "update_extent: %v %v %v\n", C.GoString(path), offset, len)
	var diff *Diff = (*Diff)(user)
	diff.tagFile(C.GoString(path), "changed")
	return 0
}

// END paste

func btrfs_read_and_process_send_stream(fd C.int, ops *C.struct_btrfs_send_ops, user unsafe.Pointer, channel chan struct{}) {
	ret, err := C.btrfs_read_and_process_send_stream(fd, ops, user, 1, 0)
	fmt.Fprintf(os.Stderr, "btrfs_read_and_process_send_stream returned %v %v\n", ret, err)
	channel <- struct{}{}
}

type Node struct {
	Children   map[string]*Node
	Name       string
	ChangeType string
	Parent     *Node
	Original   *Node
}

type Diff struct {
	Original Node
	New      Node
}

func (diff *Diff) tagFile(path string, changeType string) {
	fmt.Fprintf(os.Stdout, "TRACE %10v %v\n", changeType, path)
	fileNode := diff.find(path, changeType == "added")
	if changeType == "deleted" {
		if fileNode.Original == nil {
			fmt.Fprintf(os.Stderr, "deleting path %v which was created in same diff?\n")
		}
		delete(fileNode.Parent.Children, fileNode.Name)
	} else { // Why this? if fileNode.Original != nil {
		if !(fileNode.ChangeType == "added" && changeType == "changed") {
			fileNode.ChangeType = changeType
		}
	}
	if changeType == "deleted" {
		// If we deleted /this/ node, it sure as hell needs no children.
		fileNode.Children = nil
		if fileNode.Original != nil {
			// Leave behind a sentinel in the Original structure.
			fileNode.Original.ChangeType = "deleted"
			fileNode.Original.verifyDelete(path)
			fileNode.Original.Children = nil
		}
	}
	//fmt.Fprintf(os.Stderr, "intermediate=%v\n", diff)
}

func (node *Node) verifyDelete(path string) {
	for _, child := range node.Children {
		if child.ChangeType != "deleted" && child.ChangeType != "renamed" {
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
		fromNode.Original.ChangeType = "renamed"
	}
	toNode := diff.find(to, true)
	toNode.Parent.Children[toNode.Name] = fromNode
	fromNode.Name = toNode.Name
	fromNode.ChangeType = "added"
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
		if newFiles[name] != nil && node.ChangeType == "" {
			if node.Children == nil {
				// TODO diff equality only
				ret = append(ret, fmt.Sprintf("%10v: %v", "changed", name))
			}
			delete(newFiles, name)
		} else {
			if node.ChangeType != "deleted" && node.ChangeType != "renamed" {
				fmt.Fprintf(os.Stderr, "unexpected ChangeType on original %v: %v", name, node.ChangeType)
			}
			if (node.ChangeType == "deleted" || node.ChangeType == "renamed") && newFiles[name] != nil && newFiles[name].ChangeType == "added" {
				ret = append(ret, fmt.Sprintf("%10v: %v", "changed", name))
				delete(newFiles, name)
			} else {
				//fmt.Fprintf(os.Stderr, "DEBUG DEBUG %v %v %v\n ", node.ChangeType, newFiles[name], name)
				ret = append(ret, fmt.Sprintf("%10v: %v", node.ChangeType, name))
			}
		}
	}
	for name := range newFiles {
		ret = append(ret, fmt.Sprintf("%10v: %v", "added", name))
	}
	return ret
}

func changes(node *Node, prefix string, ret map[string]*Node) {
	newPrefix := prefix + node.Name
	ret[newPrefix] = node
	if node.ChangeType == "added" {
		// TODO diff equality only
		return
	}
	for _, child := range node.Children {
		changes(child, newPrefix+"/", ret)
	}
}

func main() {
	send_ops := C.struct_btrfs_send_ops{}
	C.setup(&send_ops)
	read, write, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipe returned %v\n", err)
		os.Exit(1)
	}

	// I need to output, in tree order, an array of Change objects.
	// need to distinguish an ChangeAdd vs ChangeModify

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
	channel := make(chan struct{})
	var diff Diff = Diff{}
	go btrfs_read_and_process_send_stream(C.int(read.Fd()), &send_ops, unsafe.Pointer(&diff), channel)
	r1, r2, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(subvol_fd), C.BTRFS_IOC_SEND, uintptr(unsafe.Pointer(&opts)))
	fmt.Fprintf(os.Stderr, "ioctl returns %v %v %v\n", r1, r2, err)
	<-channel

	//ret = C.btrfs_read_and_process_send_stream(C.int(os.Stdin.Fd()), &send_ops, unsafe.Pointer(&count), 1, 10)
	//ret = C.btrfs_read_and_process_send_stream(C.int(os.Stdin.Fd()), &send_ops, unsafe.Pointer(&count), 1, 10)
	//ret = C.btrfs_read_and_process_send_stream(C.int(os.Stdin.Fd()), &send_ops, unsafe.Pointer(&count), 1, 10)
	//if ret < 0 {
	//	fmt.Fprintf(os.Stderr, "btrfs_read_and_process_send_stream returned %v\n", ret)
	//	os.Exit(1)
	//}
	fmt.Fprintf(os.Stdout, "TRACE GENERATED\nTRACE %v\n", strings.Join(diff.Changes(), "\nTRACE "))
}
