#!/usr/bin/python3

"""一个工作在HFS应用层的示例程序，将文件系统中的给定目录或文件制作成MapNode
和FileNode构成的快照。它产生的结果并不符合HFS的推荐用法，但作为一种存档和备
份，有一定的实用意义。之后只要不修改文件内容，建立新的层次结构来整理快照中
的文件并不会显著增加HFS的占用空间。它会将链接视作其所指向的目标，而忽略除了
目录、普通文件、链接以外的一切文件系统对象。它会将Windows快捷方式视作含有二
进制内容的普通文件，因为Windows就是这样向应用程序报告的。

    Usage: snapshot.py <local pool path> [<option(s)>] <target(s)>

注意：默认行为下，这个程序认为文件系统中的文件名和文件元信息均与文件本身无
关，因此会忽略一切元信息，只将文件名存储在相应的MapNode对象中。换句话说，它
创建出的节点皆不会带有任何属性。以下命令行选项可为生成的节点添加属性：

    -Ftitle     将不含扩展名的文件名作为文件的title属性
    -Ftype      从扩展名推断文件的type属性，无法推断则不设置此属性
                    这个选项目前还无法推断任何属性
    -Fexec      推断文件是否为可执行文件，若是则设置文件的exec属性
    -Ftime      将文件的最后修改时间作为time属性
    -Fctime     将文件的ctime作为time属性，请注意这个概念依赖于操作系统
    -Fmode      按照POSIX权限概念设置文件的uid、gid和access属性
    -Dtitle     将目录名作为目录的title属性
    -Dtime      将目录的最后修改时间作为time属性
    -Dctime     将目录的ctime作为time属性，请注意这个概念依赖于操作系统
    -Dmode      按照POSIX权限概念设置目录的uid、gid和access属性

另外，如果您已经为某个目录制作了快照，在为其祖先目录制作快照时，或许会希望
利用之前已经制作好的快照。这个程序会在每个目录中寻找“.hfssnapshot”文件，将
其中的哈希值作为本目录对应节点的哈希值直接使用。

    -s          完成快照后在顶层目录留下哈希值文件
    -S          每个子目录完成快照后立刻在其中留下哈希值文件
    -f          不采纳哈希值文件给出的信息、不处理哈希值文件
    -F          将哈希值文件也视为普通文件进行处理

这个程序也支持以下命令行参数：

    --          将此后的命令行参数皆视为要处理的目录或文件名
    -h, --help  打印此帮助信息
    --debug     在出错时打印调用栈以便调试
"""

__version__ = '0.1'
__all__ = ['snapshot']
__author__ = 'Hypercube <hypercube@0x01.me>'

from pathlib import Path

from hfs import HFS, LocalPool, MapNode, FileNode


def guess_type(path):
    path = Path(path)
    return None


def snapshot(hfs, path, *, file_attrs=None, dir_attrs=None,
             use_hashfile=True, process_hashfile=False,
             leave_hashfile=(False, False),
             keys=None):
    """See the module docstring and source code."""
    if keys is None:
        keys = {}
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(2, 'No such file or directory', str(path))
    stat = path.stat()
    inode = stat.st_dev, stat.st_ino
    if inode in keys:
        if keys[inode]:
            return keys[inode]
        raise ValueError('%s: A symlink to its parent.' % path)
    e9 = 1000000000

    if path.is_file():
        print('F:', path)
        attrs = {}
        for attr in file_attrs:
            if attr == 'title':
                attrs['title'] = path.stem
            elif attr == 'type':
                t = guess_type(path)
                if t:
                    attrs['type'] = t
            elif attr == 'exec':
                if stat.st_mode & 0o111:
                    attrs['exec'] = 'true'
            elif attr == 'time':
                attrs['time'] = str(stat.st_mtime_ns // e9)
                if stat.st_mtime_ns % 1e9:
                    attrs['time'] += '.%09d' % (stat.st_mtime_ns % 1e9)
            elif attr == 'ctime':
                attrs['time'] = str(stat.st_ctime_ns // e9)
                if stat.st_ctime_ns % 1e9:
                    attrs['time'] += '.%09d' % (stat.st_ctime_ns % 1e9)
            elif attr == 'mode':
                attrs['uid'] = str(stat.st_uid)
                attrs['gid'] = str(stat.st_gid)
                attrs['access'] = '%o' % stat.st_mode
            else:
                raise AttributeError('Unsupported file attr: %s' % attr)
        with path.open('rb') as f:
            data = hfs(f)
        keys[inode] = hfs(FileNode(data, **attrs))
        return keys[inode]

    if path.is_dir():
        print('D:', path)
        if use_hashfile:
            hf = path / '.hfssnapshot'
            if hf.is_file():
                keys[inode] = hf.read_text().strip().lower()
                return keys[inode]
        keys[inode] = None
        data = {}
        for item in path.iterdir():
            if item.name == '.hfssnapshot' and not process_hashfile:
                continue
            key = snapshot(hfs, item,
                    file_attrs=file_attrs,
                    dir_attrs=dir_attrs,
                    use_hashfile=use_hashfile,
                    process_hashfile=process_hashfile,
                    leave_hashfile=leave_hashfile[1:]*2,
                    keys=keys)
            if key:
                data[item.name] = key
        attrs = {}
        for attr in dir_attrs:
            if attr == 'title':
                attrs['title'] = path.name
            elif attr == 'time':
                attrs['time'] = str(stat.st_mtime_ns // e9)
                if stat.st_mtime_ns % 1e9:
                    attrs['time'] += '.%09d' % (stat.st_mtime_ns % 1e9)
            elif attr == 'ctime':
                attrs['time'] = str(stat.st_ctime_ns // e9)
                if stat.st_ctime_ns % 1e9:
                    attrs['time'] += '.%09d' % (stat.st_ctime_ns % 1e9)
            elif attr == 'mode':
                attrs['uid'] = str(stat.st_uid)
                attrs['gid'] = str(stat.st_gid)
                attrs['access'] = '%o' % stat.st_mode
            else:
                raise AttributeError('Unsupported dir attr: %s' % attr)
        keys[inode] = hfs(MapNode(data, **attrs))
        hfs.flush()
        if leave_hashfile[0]:
            (path / '.hfssnapshot').write_text(keys[inode] + '\n')
        return keys[inode]

    return None


if __name__ == '__main__':
    from sys import argv, exit, stderr

    if len(argv) < 2 or '-h' in argv[1:] or '--help' in argv[1:]:
        print(__doc__)
        exit()

    hfs = HFS(LocalPool(argv[1]))
    targets = []
    file_attrs = set()
    dir_attrs = set()
    use_hashfile = True
    process_hashfile = False
    leave_hashfile = False, False
    force_target = False
    met_target = False
    debug = False
    for arg in argv[2:]:
        if force_target:
            targets.append(arg)
        elif not arg.startswith('-'):
            targets.append(arg)
            met_target = True
        elif met_target:
            exit('Options must come before targets. Abort.')
        elif arg.startswith('-F'):
            file_attrs.add(arg[2:])
        elif arg.startswith('-D'):
            dir_attrs.add(arg[2:])
        elif arg == '-s':
            leave_hashfile = True, False
        elif arg == '-S':
            leave_hashfile = True, True
        elif arg == '-f':
            use_hashfile = False
        elif arg == '-F':
            use_hashfile = False
            process_hashfile = True
        elif arg == '--debug':
            debug = True
        elif arg == '--':
            force_target = True
        else:
            exit('Unrecognized option: %r. Abort.' % arg)

    error = 0
    for target in targets:
        try:
            print(snapshot(hfs,
                           target,
                           file_attrs=file_attrs,
                           dir_attrs=dir_attrs,
                           use_hashfile=use_hashfile,
                           process_hashfile=process_hashfile,
                           leave_hashfile=leave_hashfile))
        except Exception as e:
            if debug:
                import traceback
                traceback.print_exc()
            print(target, 'failed:', type(e).__name__+':', e, file=stderr)
            error += 1
    if error == 1:
        exit('Failed to snapshot a target!')
    elif error:
        exit('Failed to snapshot %d targets!' % error)
