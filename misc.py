
# check_list = "test"
#
# def check_func(check_var):
#     check_var.capitalize()
#
# print(check_list)
#
# check_func(check_list)
#
# print(check_list.capitalize())

class checkCls():

    clsVar = 5

    def __init__(self):
        self.check = 5

    def check_method(self):
        clsVar = 10
        self.check = 10

checkObj = checkCls()
checkObj2 = checkCls()

print(checkObj2.clsVar)

checkObj.clsVar = 10
checkObj.new_var = 20

print(checkObj.new_var)

