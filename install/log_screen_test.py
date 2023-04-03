import time

from log_screen import LogScreen


def main():
    ls = LogScreen()
    try:
        with ls:
            test_task = ls.add_task("Test Task")
            task2 = ls.add_task(f"Second Task")
            test_task.start()
            sub_task_a = test_task.add_subtask("Subtask A")
            sub_task_b = test_task.add_subtask("Subtask B")
            sub_task_c = test_task.add_subtask("Subtask C")
            sub_task_d = test_task.add_subtask("Subtask D")
            for i in range(30):
                line = f"This is Line {i+1}"
                if i < 10:
                    test_task.info(line)
                    time.sleep(0.1)
                elif i < 20:
                    if i == 10:
                        test_task.info("Starting Subtask A")
                        sub_task_a.start()
                    sub_task_a.info(line)
                    time.sleep(0.1)
                else:
                    if i == 20:
                        sub_task_a.success()
                        test_task.info("Finished Subtask A")
                        sub_task_b.start()
                        sub_task_c.start()
                        sub_task_d.start()
                    sub_task_b.info(line)
                    sub_task_c.info(line)
                    sub_task_d.info(line)
            time.sleep(15)
            sub_task_b.success()
            sub_task_c.success()
            sub_task_d.success()
            test_task.success()
            task2.start()
            time.sleep(1)
            task2.warning("Oh no!")
            time.sleep(3)
            task2.error("An error occurred")
            task2.failed()
            time.sleep(5)
    except Exception as e:
        print(f"{e=}")
        raise


if __name__ == '__main__':
    main()
