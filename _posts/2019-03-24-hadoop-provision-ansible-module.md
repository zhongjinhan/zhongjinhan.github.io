---
layout:     post
title:      集群新增扩容中常用的Ansible模块
date:       2019-03-24
summary:    罗列了一些在Hadoop集群新建或扩容时常用的ansible模块
categories: ansible 
published:  true
---



虽然现在有现成的工具比较方便地管理/扩展集群，比如ClouderaManager和Ambari, 但是在使用这些工具之前，不可避免地需要对节点服务器做一些预处理/设置，这个时候可以用ansible这个自动化运维工具作一些批处理操作以便增加生产效率


## Ansible安装与配置

常规安装，比如在CentOS下，先安装epel源，然后在执行安装命令即可
```
sudo yum install -y ansible
```

ansible的默认配置文件是/etc/ansible/ansible.cfg, 但是假设当前文件夹下面也由这个文件，那么会先考虑当前文件夹，当然还可以通过环境变量或者home目录下的.ansible.cfg去指定这个文件的路径，选择一种你自己喜欢的方式就可以了。


配置文件有几个关键的参数需要调整
- inventory: 存放目标管理节点的文件路径，如果你的ansible.cfg和hosts都放在当前路径，则inventory直接等于hosts
- forks: 增加运行效率用的，相当于ansible执行任务的并行数，默认是5，可以根据实际机器性能适量往上调，比如15或者20
- ssh_args: 增加-o StrictHostKeyChecking=no, 主要是ssh到新的机器上面的时候自动把机器加到known_host里面。
- pipelining: 设置为True, 同样可以用来提供性能，但是用这个特性的前提目标机器允许执行sudo的时候没有tty,具体是把/etc/sudoers里面的Defaults    requiretty注释掉。比如可以用如下的ansible任务 

    ```
    - hosts: all
    become: yes
    tasks:
        - name: disable require tty
        lineinfile:
            dest: /etc/sudoers
            state: absent
            regexp: '^Defaults.+requiretty'
    ```

样例配置如下，前面两个配置项在defaults下面，后两者在ssh_connection下面:
```
[defaults]
inventory      = hosts
forks          = 2

[ssh_connection]
ssh_args= -o StrictHostKeyChecking=no
pipelining = True
```

## 创建用户
一般来说我们需要创建专门的管理账号来管理集群，并且为账号增加sudo权限和免密码连接配置，可以用如下一系列任务来完成。

这里有几点要注意：
1. 用户密码是hash过后的值，具体方法可看[这里](https://docs.ansible.com/ansible/latest/reference_appendices/faq.html#how-do-i-generate-crypted-passwords-for-the-user-module)
2. sudo 设置放在/etc/sudoers.d/admin这个文件里面
3. 免密设置里面的pubkey位置指向~/.ssh/id_rsa.pub

```
- hosts: all
  become: yes
  tasks:
    - name: create user xadmin
      user:
        name: xadmin
        shell: /bin/bash
        state: present
        password: $6$EE63uMRPL8h9YdW$5czknuSgaBSZgBGVZCy11eKCg.yCzw6i9494qRbtARrMsD/6PIABYlnis7ytyXF7BHLlhkg/9NgoURo15dGyh0

    - name: check if /etc/sudoers.d/admin  exists
      stat:
        path: /etc/sudoers.d/admin
      register: stat_result

    - name: touch /etc/sudoers.d/admin if not exist
      file:
        path: /etc/sudoers.d/admin
        state: touch
        mode: 0400
      when: stat_result.stat.exists == False

    - name: add xadmin to sudo
      lineinfile:
        dest: /etc/sudoers.d/admin
        state: present
        regexp: '^xadmin'
        line: 'xadmin ALL=(ALL) NOPASSWD: ALL'

    - name: Add public key for user
      authorized_key:
        user: xadmin
        key: "{{ "{{lookup('file', '~/.ssh/id_rsa.pub') "}}}}"

```




## 安装与服务
在用ambari或者cloudera manager来增加服务器之前，有些额外的软件包需要安装，比如rsync，ntp，kerberos客户端之类的，下面这个playbook列出了在CentOS下面一系列相关的task,主要完成了:
1. 拷贝local.repo 文件到被管理机器上，一般情况下我们会搭建内部yum repo,然后给出一个本地的repo文件
2. 安装ntp,rsync和krb5-workstation,
3. 拷贝文件ntp和kerberos配置文件
4. 启动ntpd服务

```
- hosts: all
  become: yes
  tasks:
    - name: copy repo file
      copy:
        src: local.repo
        dest: /etc/yum.repos.d/local.repo
        owner: root
        group: root

    - name: install packages
      yum:
        name: "{{ "{{packages "}}}}"
        enablerepo: base,updates,extras,epel,centosplus
        state: present
      vars:
        packages:
        - ntp
        - rsync
        - krb5-workstation
    - name: copy npt file
      copy:
        src: ntp.conf
        dest: /etc/ntp.conf
        owner: root
        group: root
    - name: copy krb5 file
      copy:
        src: krb5.conf
        dest: /etc/krb5.conf
        owner: root
        group: root
    - name: nptd
      service:
        name: ntpd
        state: restarted
        enabled: yes
```
## 文件同步
同步单个文件可以简单地使用copy任务(上面同步配置文件的即使用了copy), 但同步大量文件就必须使用rsync了，copy的效率实在太低，而ansible的synchronize模块就是使用了rsync，所以在使用这个模块之前，必须要在所有相关机器上面装上rsync。

下面是用synchronize模块同步jdk:
```
- hosts: all
  become: yes
  tasks:
    - name: rsync jdk
      synchronize:
        src: jdk1.8.0_131
        dest: /var/lib/
```


## /etc/hosts 和 hostname 修改

如果没有DNS,我们经常会用/etc/hosts来配置FQDN，那么集群扩容需要修改每一台机器的/etc/hosts，这时候可以用ansible里面的lineinfile或者blockinfile, 如果是新的集群可以直接用blockinfile, 操作简单并且执行速度快，可以直接从一个本地文件里面读取然后放到被管理机器的/etc/hosts里面，并且前面加上标记，示例如下：

```
- hosts: all
  become: yes
  tasks:
    - name: Add mappings to /etc/hosts
      blockinfile:
        path: /etc/hosts
        block: "{{ "{{lookup('file', './host-mapping') "}}}}"
        marker: "# {mark} ANSIBLE MANAGED BLOCK- HOST MAPPING"
```

因为blockinfile是以块来更新的，所以假如/etc/hosts已经有映射并且之前不是用blockinfile来更新的，那么这个时候用blockinfile来管理就不是那么简单了，需要用lineinfile模块，在这里面我们可以用正则表达式（regexp的值)查找到一条映射并且用line的值去更新它，有多条记录需要改的时候可以使用with_items来循环运行，这个方法看起来就要繁琐很多了，有50台机器就有50行替换记录，在实际运行变更的时候，随着机器数量越多，性能会下降的比较厉害。


```
- hosts: all
  become: yes
  tasks:
    - name: "change hosts"
      lineinfile:
        dest: /etc/hosts
        regexp: "{{ "{{item.regexp "}}}}"
        line: "{{ "{{item.line "}}}}"
      with_items:
        - { regexp: '^192.168.1.1 ', line: '192.168.1.1     node01.example.com    node01' }
        - { regexp: '^192.168.1.2 ', line: '192.168.1.2     node02.example.com    node01' }
        - { regexp: '^192.168.1.3 ', line: '192.168.1.3     node03.example.com    node01' }
```





关于修改hostname目前没找到更方便的方法，因为毕竟这个动作不会经常执行，所以是采用字符串拼接的方式用ssh去执行, 比如在CentOS下面，我们可以采用如下命令


```
ssh 192.168.1.1 sudo hostnamectl set-hostname node01.example.com
ssh 192.168.1.2 sudo hostnamectl set-hostname node02.example.com
ssh 192.168.1.3 sudo hostnamectl set-hostname node03.example.com
```




