    AREA |.rodata|, DATA, READONLY
; 2D Array: 3 rows x 3 columns (stored in row-major form)
matrix2d   DCD   1, 2, 3      ; Row 0
           DCD   4, 5, 6      ; Row 1  
           DCD   7, 8, 9      ; Row 2

rows       EQU   3            ; number of rows
cols       EQU   3            ; number of columns

    AREA |.data|, DATA, READWRITE
ans        DCD   0            ; Storage for accessed element


    AREA |.text|, CODE, READONLY
    ENTRY
    EXPORT main

main
    ; Initialize base address
    LDR R5, =matrix2d          ; R5 = base address of 2D array
    LDR R4, =cols              
    LDR R3, =rows

    ; Access element at matrix[2][3] (value will be 6)
    MOV R1, #1                 ; i = row index = 1 
    MOV R2, #2                 ; j = column index = 2
    
    
    ; 2D array formula(row major): Offset = i * number of columns + j
    MUL R6, R4, R1             ; R6 = i * cols          
    ADD R7, R6, R2             ; R6 = i * cols + j
    
    ; Address(A[i][j]) = Base Address + Offset * element size
    LDR R8, [R5, R7, LSL #2]   
    
    LDR R0, =ans
    STR R8, [R0]
    
    
    B stop

stop
    B stop
    END