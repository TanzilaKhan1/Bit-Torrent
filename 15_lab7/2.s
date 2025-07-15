    AREA |.rodata|, DATA, READONLY
; Matrix A: 2x2
matrixA   DCD  1, 2            ; Row 0
          DCD  5, 6            ; Row 1

; Matrix B: 2x2  
matrixB   DCD  3, 4            ; Row 0
          DCD  7, 8            ; Row 1

rows      EQU   2
cols      EQU   2


    AREA |.data|, DATA, READWRITE
; Result matrix C = A * B
matrixC   DCD  0, 0            ; Row 0
          DCD  0, 0            ; Row 1


    AREA |.text|, CODE, READONLY
    ENTRY
    EXPORT main

main
    ; Load base addresses
    LDR R5, =matrixA           ; R5 = base address of matrix A
    LDR R6, =matrixB           ; R6 = base address of matrix B
    LDR R7, =matrixC           ; R7 = base address of result matrix C


    ; Outer loop: i (rows of A)
    MOV R0, #0                 ; i (row counter) = 0
outer_loop
    CMP R0, #rows
    BGE matrix_mult_done
    

    ; Middle loop: j (columns of B)
    MOV R1, #0                 ; j (column counter) = 0
middle_loop
    CMP R1, #cols
    BGE next_row
    
    ; Initialize sum for C[i][j]
    MOV R8, #0                 ; sum = 0
    LDR R12, =cols

    ; Inner loop: k (columns of A / rows of B)
    MOV R2, #0                 ; k = 0
inner_loop
    CMP R2, #cols
    BGE store_result
    
    ; Calculate A[i][k]
    MUL R3, R0, R12          
    ADD R3, R3, R2             ; R3 = i * cols + k
    LDR R9, [R5, R3, LSL #2]   ; R9 = A[i][k]
    
    ; Calculate B[k][j]
    MUL R4, R2, R12          
    ADD R4, R4, R1             ; R4 = k * cols + j
    LDR R10, [R6, R4, LSL #2]  ; R10 = B[k][j]
    
    ; Multiply and accumulate
    MUL R11, R9, R10           ; R11 = A[i][k] * B[k][j]
    ADD R8, R8, R11            ; sum += R11
    
    ADD R2, R2, #1             ; k++
    B inner_loop

next_row
    ADD R0, R0, #1             ; i++
    B outer_loop


store_result
    ; Store C[i][j] = sum
    MUL R3, R0, R12          
    ADD R3, R3, R1             ; R3 = i * cols + j
    STR R8, [R7, R3, LSL #2]   
    
    ADD R1, R1, #1             ; j++
    B middle_loop


matrix_mult_done
    B stop

stop
    B stop
    END