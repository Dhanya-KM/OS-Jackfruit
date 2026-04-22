/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Credits & Implementation:
 * - Device registration and teardown: Provided Boilerplate
 * - Timer setup: Provided Boilerplate
 * - RSS helper: Provided Boilerplate
 * - Soft-limit and hard-limit event helpers: Provided Boilerplate
 * - ioctl dispatch shell: Provided Boilerplate
 * - Linked-list node struct & Global list: Implemented by Dhanya K.M.
 * - Periodic monitoring (Timer Callback): Implemented by Dhanya K.M.
 * - Add/Remove monitored entry (IOCTL): Implemented by Dhanya K.M.
 * - Memory cleanup (Module Exit): Implemented by Dhanya K.M.
 *
 * YOUR WORK: All sections marked // TODO are completed below.
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ==============================================================
 * TODO 1: Define your linked-list node struct.
 *
 * Requirements:
 * - track PID, container ID, soft limit, and hard limit
 * - remember whether the soft-limit warning was already emitted
 * - include `struct list_head` linkage
 * ============================================================== */
struct container_node {
    pid_t pid;
    char container_id[32];
    unsigned long soft_limit;
    unsigned long hard_limit;
    bool soft_warning_emitted;
    struct list_head list;
};

/* ==============================================================
 * TODO 2: Declare the global monitored list and a lock.
 *
 * Requirements:
 * - shared across ioctl and timer code paths
 * - protect insert, remove, and iteration safely
 * ============================================================== */
static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(list_lock);

/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 *
 * Returns the Resident Set Size in bytes for the given PID,
 * or -1 if the task no longer exists.
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit helper
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit helper
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Timer Callback - fires every CHECK_INTERVAL_SEC seconds.
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    /* ==============================================================
     * TODO 3: Implement periodic monitoring.
     * ============================================================== */
    struct container_node *node, *tmp;
    long current_rss;

    mutex_lock(&list_lock);
    list_for_each_entry_safe(node, tmp, &monitored_list, list) {
        current_rss = get_rss_bytes(node->pid);

        /* Remove if process is gone */
        if (current_rss < 0) {
            list_del(&node->list);
            kfree(node);
            continue;
        }

        /* Check hard limit */
        if (current_rss > node->hard_limit) {
            kill_process(node->container_id, node->pid, node->hard_limit, current_rss);
            list_del(&node->list);
            kfree(node);
            continue;
        }

        /* Check soft limit */
        if (current_rss > node->soft_limit && !node->soft_warning_emitted) {
            log_soft_limit_event(node->container_id, node->pid, node->soft_limit, current_rss);
            node->soft_warning_emitted = true;
        }
    }
    mutex_unlock(&list_lock);

    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;
    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        /* ==============================================================
         * TODO 4: Add a monitored entry.
         * ============================================================== */
        struct container_node *node;
        node = kmalloc(sizeof(*node), GFP_KERNEL);
        if (!node) return -ENOMEM;

        node->pid = req.pid;
        strncpy(node->container_id, req.container_id, sizeof(node->container_id) - 1);
        node->container_id[sizeof(node->container_id) - 1] = '\0';
        node->soft_limit = req.soft_limit_bytes;
        node->hard_limit = req.hard_limit_bytes;
        node->soft_warning_emitted = false;

        mutex_lock(&list_lock);
        list_add(&node->list, &monitored_list);
        mutex_unlock(&list_lock);

        return 0;
    }

    /* ==============================================================
     * TODO 5: Remove a monitored entry on explicit unregister.
     * ============================================================== */
    if (cmd == MONITOR_UNREGISTER) {
        struct container_node *node, *tmp;
        int found = 0;

        mutex_lock(&list_lock);
        list_for_each_entry_safe(node, tmp, &monitored_list, list) {
            if (node->pid == req.pid) {
                list_del(&node->list);
                kfree(node);
                found = 1;
                break;
            }
        }
        mutex_unlock(&list_lock);
        return found ? 0 : -ENOENT;
    }

    return -ENOENT;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    /* FIX: Declarations moved to the top of the function to avoid
     * mixing declarations and statements, which is invalid in C89/C90
     * and may cause warnings or errors under strict kernel build configs. */
    struct container_node *node, *tmp;

    /* * IMPORTANT FIX for Kernel 6.17:
     * Use timer_shutdown_sync instead of del_timer_sync
     */
    timer_shutdown_sync(&monitor_timer);

    /* ==============================================================
     * TODO 6: Free all remaining monitored entries.
     * ============================================================== */
    mutex_lock(&list_lock);
    list_for_each_entry_safe(node, tmp, &monitored_list, list) {
        list_del(&node->list);
        kfree(node);
    }
    mutex_unlock(&list_lock);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Dhanya K.M., Nidhi R.");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
